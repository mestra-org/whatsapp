package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/ptr"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// kvGhostPhonesSynced is set once every LID ghost with a known phone number has it in its profile.
const kvGhostPhonesSynced = "mestra_lid_ghost_phones_synced"

// ghostPhonesDelay paces the profile writes, since each ghost costs a few requests to the homeserver.
const ghostPhonesDelay = 20 * time.Millisecond

// syncLIDGhostPhones puts the phone number of every LID ghost the bridge can map to one into the
// ghost's profile, as a tel: identifier. Ghosts normally only get it on their next info resync,
// which is up to ResyncMinInterval away, so ghosts created before the profile settings were turned
// on would otherwise go without it for days. Runs once, and only when the identifiers can be published.
func (wa *WhatsAppConnector) syncLIDGhostPhones(ctx context.Context) error {
	if wa.Bridge.Background || wa.Bridge.DB.KV.Get(ctx, kvGhostPhonesSynced) == "true" {
		return nil
	}
	caps := wa.Bridge.Matrix.GetCapabilities()
	if !wa.Bridge.Config.PhoneNumbersInProfile || (!caps.ExtraProfileMeta && !caps.ReplaceEntireProfile) {
		return nil
	}
	const findLIDGhostsWithPN = `
		SELECT ghost.id, lid_map.pn
		FROM ghost
		JOIN whatsmeow_lid_map lid_map ON lid_map.lid=replace(ghost.id, 'lid-', '')
		WHERE ghost.bridge_id=$1 AND ghost.id LIKE 'lid-%'
	`
	type lidGhost struct {
		id networkid.UserID
		pn string
	}
	rows, err := dbutil.ConvertRowFn[lidGhost](func(row dbutil.Scannable) (g lidGhost, err error) {
		err = row.Scan(&g.id, &g.pn)
		return
	}).NewRowIter(wa.Bridge.DB.Query(ctx, findLIDGhostsWithPN, wa.Bridge.ID)).AsList()
	if err != nil {
		return fmt.Errorf("failed to get LID ghosts with phone numbers: %w", err)
	}
	log := zerolog.Ctx(ctx)
	log.Info().Int("ghost_count", len(rows)).Msg("Putting phone numbers in LID ghost profiles")
	updated := 0
	for _, row := range rows {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ghost, err := wa.Bridge.GetGhostByID(ctx, row.id)
		if err != nil {
			return fmt.Errorf("failed to get ghost %s: %w", row.id, err)
		}
		identifiers := []string{"tel:+" + row.pn}
		if ghost.ContactInfoSet && len(ghost.Identifiers) > 0 {
			continue
		}
		ghost.UpdateInfo(ctx, &bridgev2.UserInfo{Identifiers: identifiers, IsBot: ptr.Ptr(ghost.IsBot)})
		updated++
		time.Sleep(ghostPhonesDelay)
	}
	log.Info().Int("updated_ghosts", updated).Msg("Finished putting phone numbers in LID ghost profiles")
	wa.Bridge.DB.KV.Set(ctx, kvGhostPhonesSynced, "true")
	return nil
}
