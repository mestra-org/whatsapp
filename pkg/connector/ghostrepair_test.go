package connector

import (
	"testing"

	"go.mau.fi/util/ptr"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

func testGhost(name string, nameSet bool, avatarID, mxc string, avatarSet bool, identifiers ...string) *bridgev2.Ghost {
	return &bridgev2.Ghost{Ghost: &database.Ghost{
		Name:        name,
		NameSet:     nameSet,
		AvatarID:    networkid.AvatarID(avatarID),
		AvatarMXC:   id.ContentURIString(mxc),
		AvatarSet:   avatarSet,
		Identifiers: identifiers,
	}}
}

func TestGhostNeedsAvatar(t *testing.T) {
	cases := []struct {
		name  string
		ghost *bridgev2.Ghost
		want  bool
	}{
		{"never fetched", testGhost("A", true, "", "", false), true},
		{"fetch failed after bridgev2 flagged it set", testGhost("A", true, "", "", true), true},
		{"ID stored but picture never reached Matrix", testGhost("A", true, "123", "", true), true},
		{"bridged", testGhost("A", true, "123", "mxc://x/y", true), false},
		{"user has no picture", testGhost("A", true, "remove", "", true), false},
		{"picture hidden by privacy settings", testGhost("A", true, "unauthorized", "", true), false},
	}
	for _, tc := range cases {
		if got := ghostNeedsAvatar(tc.ghost); got != tc.want {
			t.Errorf("%s: ghostNeedsAvatar = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestAllowAvatarRetry(t *testing.T) {
	stuck := testGhost("A", true, "", "", true)
	if !allowAvatarRetry(stuck) || stuck.AvatarSet {
		t.Errorf("stuck ghost: want AvatarSet cleared and a save, got AvatarSet=%v", stuck.AvatarSet)
	}
	bridged := testGhost("A", true, "123", "mxc://x/y", true)
	if allowAvatarRetry(bridged) || !bridged.AvatarSet {
		t.Error("ghost with a picture must keep AvatarSet")
	}
	alreadyRetrying := testGhost("A", true, "", "", false)
	if allowAvatarRetry(alreadyRetrying) {
		t.Error("ghost already open for retry shouldn't need a save")
	}
}

func TestGhostOutdated(t *testing.T) {
	info := &bridgev2.UserInfo{Name: ptr.Ptr("Ali"), Identifiers: []string{"tel:+989120000000"}}
	cases := []struct {
		name  string
		ghost *bridgev2.Ghost
		want  bool
	}{
		{"never synced", testGhost("", false, "", "", false), true},
		{"name not pushed to Matrix", testGhost("Ali", false, "1", "mxc://x/y", true, "tel:+989120000000"), true},
		{"name changed", testGhost("Old", true, "1", "mxc://x/y", true, "tel:+989120000000"), true},
		{"phone number missing", testGhost("Ali", true, "1", "mxc://x/y", true), true},
		{"photo missing", testGhost("Ali", true, "", "", true, "tel:+989120000000"), true},
		{"up to date", testGhost("Ali", true, "1", "mxc://x/y", true, "tel:+989120000000"), false},
		{"up to date, no photo", testGhost("Ali", true, "remove", "", true, "tel:+989120000000"), false},
	}
	for _, tc := range cases {
		if got := ghostOutdated(tc.ghost, info); got != tc.want {
			t.Errorf("%s: ghostOutdated = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestGhostRepairQueue(t *testing.T) {
	wa := &WhatsAppClient{}
	// Pretend the worker is already running so queueing doesn't start one.
	wa.ghostRepair.running = true
	a := types.NewJID("111", types.HiddenUserServer)
	b := types.NewJID("222", types.DefaultUserServer)
	aDevice := a
	aDevice.Device = 3

	wa.queueGhostRepair(a, b, aDevice)
	var got []types.JID
	for {
		jid, ok := wa.nextGhostRepair()
		if !ok {
			break
		}
		got = append(got, jid)
	}
	if len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("queue order = %v, want [%v %v] with the device JID deduplicated", got, a, b)
	}
	if wa.ghostRepair.running {
		t.Error("draining the queue should mark the worker stopped")
	}

	wa.ghostRepair.running = true
	for range ghostRepairMaxAttempts + 2 {
		wa.queueGhostRepair(a)
		if _, ok := wa.nextGhostRepair(); !ok {
			break
		}
		wa.ghostRepair.running = true
	}
	if n := wa.ghostRepair.attempts[a]; n != ghostRepairMaxAttempts {
		t.Errorf("attempts = %d, want capped at %d", n, ghostRepairMaxAttempts)
	}

	// A rate-limited attempt is put back without using up an attempt.
	wa.ghostRepair.running = true
	wa.queueGhostRepair(b)
	jid, _ := wa.nextGhostRepair()
	before := wa.ghostRepair.attempts[b]
	wa.requeueGhostRepair(jid)
	if wa.ghostRepair.attempts[b] != before-1 {
		t.Errorf("requeue should refund the attempt")
	}
	if jid, ok := wa.nextGhostRepair(); !ok || jid != b {
		t.Errorf("requeued ghost should come back next, got %v %v", jid, ok)
	}
}
