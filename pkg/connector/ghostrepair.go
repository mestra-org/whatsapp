package connector

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// Ghost profile repair.
//
// bridgev2 only asks the connector for a ghost's info while the ghost's AvatarSet flag is false,
// and Ghost.UpdateInfo sets that flag before our avatar fetch (an ExtraUpdates func) runs. So when
// fetching a WhatsApp profile picture fails, the ghost keeps no photo and is never asked about
// again. WhatsApp rate-limits these fetches, and a new login's history sync creates hundreds of
// rooms at once, so after a login a large share of ghosts can end up stuck like this. The same
// happened when a fetch had stored an avatar ID but no MXC.
//
// This file retries those ghosts in the background, one at a time, and lets the contact list sync
// the ghosts it is about to return.

const (
	// ghostRepairGap spaces out background profile picture fetches so they don't trip WhatsApp's
	// rate limit themselves.
	ghostRepairGap = 750 * time.Millisecond
	// ghostRepairRateLimitBackoff is how long the background repair pauses after WhatsApp says
	// rate-overlimit.
	ghostRepairRateLimitBackoff = 2 * time.Minute
	// ghostRepairTimeout bounds one ghost's sync.
	ghostRepairTimeout = 20 * time.Second
	// ghostRepairMaxAttempts stops a ghost whose fetch keeps failing for reasons other than rate
	// limits from being retried forever. The startup sweep tries it again after a restart.
	ghostRepairMaxAttempts = 3
	// ghostRepairSweepDelay lets the connect-time syncs (history sync, contact resync) finish
	// before the sweep looks for ghosts that are still missing a photo.
	ghostRepairSweepDelay = 3 * time.Minute
	// ghostRepairSweepInterval is how often the sweep runs again while connected.
	ghostRepairSweepInterval = 24 * time.Hour
	// ghostNoAvatarRecheckAge is how old a "no picture" or "hidden picture" answer has to be before
	// the sweep asks WhatsApp again. Picture change events can be missed while the bridge is
	// disconnected, and bridgev2 never asks again once it has an answer.
	ghostNoAvatarRecheckAge = 3 * 24 * time.Hour

	// contactSyncWorkers is how many contact list ghosts are synced at once.
	contactSyncWorkers = 6
	// contactSyncBudget is how long a contact list request waits for ghost syncs. Ghosts that
	// don't fit are left to the background repair and show up on the next listing.
	contactSyncBudget = 12 * time.Second
)

// ghostNeedsAvatar reports whether no profile picture has been bridged for the ghost and WhatsApp
// hasn't said that there is none to bridge.
func ghostNeedsAvatar(ghost *bridgev2.Ghost) bool {
	switch ghost.AvatarID {
	case "":
		return true
	case "remove", "unauthorized":
		return false
	default:
		return ghost.AvatarMXC == ""
	}
}

// allowAvatarRetry clears AvatarSet on a ghost that has no photo, so that bridgev2's
// UpdateInfoIfNecessary asks for its info again on the next event. It reports whether the ghost
// changed and needs saving.
func allowAvatarRetry(ghost *bridgev2.Ghost) bool {
	if ghost.AvatarMXC == "" && ghost.AvatarSet {
		ghost.AvatarSet = false
		return true
	}
	return false
}

type ghostRepairQueue struct {
	lock     sync.Mutex
	order    []types.JID
	pending  map[types.JID]struct{}
	recheck  map[types.JID]bool
	attempts map[types.JID]int
	running  bool
	swept    bool
}

// queueGhostRepair schedules a background sync of the ghosts' info and profile pictures.
// Duplicates and ghosts that already failed too often are ignored.
func (wa *WhatsAppClient) queueGhostRepair(jids ...types.JID) {
	wa.queueGhostRepairWith(false, jids...)
}

// queueGhostRepairWith is queueGhostRepair, optionally asking WhatsApp for the picture again even
// when the ghost already has an answer ("no picture", "hidden").
func (wa *WhatsAppClient) queueGhostRepairWith(recheck bool, jids ...types.JID) {
	q := &wa.ghostRepair
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.pending == nil {
		q.pending = make(map[types.JID]struct{})
		q.recheck = make(map[types.JID]bool)
		q.attempts = make(map[types.JID]int)
	}
	added := false
	for _, jid := range jids {
		jid = jid.ToNonAD()
		if _, ok := q.pending[jid]; ok || q.attempts[jid] >= ghostRepairMaxAttempts {
			continue
		}
		q.pending[jid] = struct{}{}
		if recheck {
			q.recheck[jid] = true
		}
		q.order = append(q.order, jid)
		added = true
	}
	if added && !q.running {
		q.running = true
		go wa.runGhostRepair()
	}
}

func (wa *WhatsAppClient) nextGhostRepair() (types.JID, bool) {
	jid, _, ok := wa.nextGhostRepairItem()
	return jid, ok
}

func (wa *WhatsAppClient) nextGhostRepairItem() (jid types.JID, recheck, ok bool) {
	q := &wa.ghostRepair
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(q.order) == 0 {
		q.running = false
		return types.EmptyJID, false, false
	}
	jid = q.order[0]
	q.order = slices.Delete(q.order, 0, 1)
	delete(q.pending, jid)
	recheck = q.recheck[jid]
	delete(q.recheck, jid)
	q.attempts[jid]++
	return jid, recheck, true
}

// requeueGhostRepair puts a ghost back without counting the attempt, for rate-limited fetches.
func (wa *WhatsAppClient) requeueGhostRepair(jid types.JID, recheck bool) {
	q := &wa.ghostRepair
	q.lock.Lock()
	defer q.lock.Unlock()
	q.attempts[jid]--
	if _, ok := q.pending[jid]; !ok {
		q.pending[jid] = struct{}{}
		if recheck {
			q.recheck[jid] = true
		}
		q.order = append(q.order, jid)
	}
}

func (wa *WhatsAppClient) clearGhostRepair() {
	q := &wa.ghostRepair
	q.lock.Lock()
	defer q.lock.Unlock()
	q.order = nil
	clear(q.pending)
	clear(q.recheck)
	q.running = false
}

func (wa *WhatsAppClient) runGhostRepair() {
	log := wa.UserLogin.Log.With().Str("action", "repair ghost profiles").Logger()
	ctx := log.WithContext(wa.Main.Bridge.BackgroundCtx)
	var synced, failed, withPicture, noPicture, hidden int
	defer func() {
		if synced > 0 || failed > 0 {
			log.Info().
				Int("synced", synced).
				Int("failed", failed).
				Int("with_picture", withPicture).
				Int("no_picture", noPicture).
				Int("hidden_picture", hidden).
				Msg("Finished repairing ghost profiles")
		}
	}()
	for {
		jid, recheck, ok := wa.nextGhostRepairItem()
		if !ok {
			return
		}
		if !wa.IsLoggedIn() {
			log.Debug().Msg("Not logged in, dropping queued ghost profile repairs")
			wa.clearGhostRepair()
			return
		}
		ghost, err := wa.Main.Bridge.GetExistingGhostByID(ctx, waid.MakeUserID(jid))
		if err != nil {
			log.Err(err).Stringer("jid", jid).Msg("Failed to get ghost to repair")
			continue
		} else if ghost == nil {
			continue
		}
		syncCtx, cancel := context.WithTimeout(ctx, ghostRepairTimeout)
		err = wa.syncGhostFromStore(syncCtx, jid, ghost, recheck)
		cancel()
		wait := ghostRepairGap
		switch {
		case err == nil:
			synced++
			switch {
			case ghost.AvatarID == "remove":
				noPicture++
			case ghost.AvatarID == "unauthorized":
				hidden++
			case ghost.AvatarMXC != "":
				withPicture++
			}
		case errors.Is(err, whatsmeow.ErrIQRateOverLimit):
			log.Warn().Stringer("jid", jid).Dur("backoff", ghostRepairRateLimitBackoff).
				Msg("Rate limited while fetching profile picture, pausing ghost repair")
			wa.requeueGhostRepair(jid, recheck)
			wait = ghostRepairRateLimitBackoff
		default:
			failed++
			log.Warn().Err(err).Stringer("jid", jid).Msg("Failed to repair ghost profile")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// syncGhostFromStore updates a ghost from the contact store and fetches its profile picture if it
// has none, then copies the result to the ghost of the user's other JID (LID or phone number).
// It returns the profile picture fetch error, if any.
func (wa *WhatsAppClient) syncGhostFromStore(ctx context.Context, jid types.JID, ghost *bridgev2.Ghost, recheck bool) error {
	contact, err := wa.GetStore().Contacts.GetContact(ctx, jid)
	if err != nil {
		return err
	}
	info := wa.contactToUserInfo(ctx, jid, contact, "", false)
	var fetchErr error
	if !wa.Main.Config.LazyAvatars {
		info.ExtraUpdates = bridgev2.MergeExtraUpdaters(info.ExtraUpdates, func(ctx context.Context, ghost *bridgev2.Ghost) bool {
			if !recheck && !ghostNeedsAvatar(ghost) {
				return false
			}
			var changed bool
			changed, fetchErr = wa.updateGhostAvatar(ctx, ghost, recheck)
			if fetchErr != nil {
				return allowAvatarRetry(ghost)
			}
			return changed
		})
	}
	ghost.UpdateInfo(ctx, info)
	wa.syncAltGhostWithInfo(ctx, jid, ghost)
	return fetchErr
}

// ghostOutdated reports whether a ghost lacks what the contact store already knows about the user
// (name, phone number) or has no profile picture yet.
func ghostOutdated(ghost *bridgev2.Ghost, info *bridgev2.UserInfo) bool {
	if info.Name != nil && (ghost.Name != *info.Name || !ghost.NameSet) {
		return true
	}
	if info.Identifiers != nil && !slices.Equal(ghost.Identifiers, info.Identifiers) {
		return true
	}
	return ghostNeedsAvatar(ghost)
}

// syncContactGhosts brings the ghosts behind a contact list up to date before it is returned,
// since bridgev2 lists contacts from what their ghosts hold and never syncs them itself. A few run
// at once within a time budget; the rest go to the background repair.
func (wa *WhatsAppClient) syncContactGhosts(ctx context.Context, contacts []*bridgev2.ResolveIdentifierResponse) {
	var stale []*bridgev2.ResolveIdentifierResponse
	for _, contact := range contacts {
		if contact.Ghost != nil && contact.UserInfo != nil && ghostOutdated(contact.Ghost, contact.UserInfo) {
			stale = append(stale, contact)
		}
	}
	if len(stale) == 0 {
		return
	}
	log := zerolog.Ctx(ctx)
	deadline := time.Now().Add(contactSyncBudget)
	var wg sync.WaitGroup
	var lock sync.Mutex
	var leftover []types.JID
	slots := make(chan struct{}, contactSyncWorkers)
	for _, contact := range stale {
		jid := waid.ParseUserID(contact.UserID)
		if time.Now().After(deadline) || ctx.Err() != nil {
			leftover = append(leftover, jid)
			continue
		}
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-slots; wg.Done() }()
			syncCtx, cancel := context.WithDeadline(ctx, deadline)
			defer cancel()
			if err := wa.syncGhostFromStore(syncCtx, jid, contact.Ghost, false); err != nil {
				lock.Lock()
				leftover = append(leftover, jid)
				lock.Unlock()
			}
		}()
	}
	wg.Wait()
	log.Debug().
		Int("contacts", len(contacts)).
		Int("stale", len(stale)).
		Int("left_for_background", len(leftover)).
		Msg("Synced contact list ghosts")
	if len(leftover) > 0 {
		wa.queueGhostRepair(leftover...)
	}
}

// startGhostRepairSweep runs the ghost profile sweep once per bridge process and login, and then
// every ghostRepairSweepInterval while the bridge runs. It covers ghosts that were left stuck
// before the repair existed, and answers from WhatsApp that may have gone stale.
func (wa *WhatsAppClient) startGhostRepairSweep() {
	q := &wa.ghostRepair
	q.lock.Lock()
	if q.swept {
		q.lock.Unlock()
		return
	}
	q.swept = true
	q.lock.Unlock()

	log := wa.UserLogin.Log.With().Str("action", "ghost profile sweep").Logger()
	ctx := log.WithContext(wa.Main.Bridge.BackgroundCtx)
	if wa.isNewLogin {
		timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		_ = wa.pushNamesSynced.Wait(timeoutCtx)
		cancel()
	}
	wait := ghostRepairSweepDelay
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = ghostRepairSweepInterval
		if wa.IsLoggedIn() {
			wa.sweepGhostProfiles(ctx)
		}
	}
}

// sweepGhostProfiles queues the ghosts of everyone in the contact store and everyone the user has
// a one-to-one chat with, when they have no profile picture yet or the "no picture" answer is old.
// DM partners are included because people outside the phone's address book often have no entry in
// the contact store.
func (wa *WhatsAppClient) sweepGhostProfiles(ctx context.Context) {
	log := zerolog.Ctx(ctx)
	contacts, err := wa.GetStore().Contacts.GetAllContacts(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to get contacts for ghost profile sweep")
		return
	}
	dmPartners, err := wa.getDMPartners(ctx)
	if err != nil {
		log.Err(err).Msg("Failed to get DM partners for ghost profile sweep")
	}
	candidates := make(map[types.JID]struct{}, len(contacts)+len(dmPartners))
	for jid := range contacts {
		candidates[jid] = struct{}{}
	}
	for jid := range dmPartners {
		candidates[jid] = struct{}{}
	}
	var missing, stale []types.JID
	for jid := range candidates {
		if jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer {
			continue
		}
		ghost, err := wa.Main.Bridge.GetExistingGhostByID(ctx, waid.MakeUserID(jid))
		if err != nil {
			log.Err(err).Stringer("jid", jid).Msg("Failed to get ghost for profile sweep")
		} else if ghost == nil {
			continue
		} else if ghostNeedsAvatar(ghost) {
			missing = append(missing, jid)
		} else if noAvatarAnswerStale(ghost, time.Now()) {
			stale = append(stale, jid)
		}
	}
	log.Info().
		Int("contacts", len(contacts)).
		Int("dm_partners", len(dmPartners)).
		Int("missing_avatar", len(missing)).
		Int("recheck_no_avatar", len(stale)).
		Msg("Queued ghosts without a profile picture for repair")
	wa.queueGhostRepair(missing...)
	wa.queueGhostRepairWith(true, stale...)
}

// noAvatarAnswerStale reports whether WhatsApp's "no picture" or "hidden picture" answer for the
// ghost is old enough to ask again.
func noAvatarAnswerStale(ghost *bridgev2.Ghost, now time.Time) bool {
	if ghost.AvatarID != "remove" && ghost.AvatarID != "unauthorized" {
		return false
	}
	meta, ok := ghost.Metadata.(*waid.GhostMetadata)
	if !ok {
		return true
	}
	return now.Sub(meta.LastSync.Time) > ghostNoAvatarRecheckAge
}
