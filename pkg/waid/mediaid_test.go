// mautrix-whatsapp - A Matrix-WhatsApp puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package waid

import (
	"bytes"
	"testing"

	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// Own messages in LID chats arrive with the LID as the sender when handled live, but
// history sync only knows the phone number. Both must produce the same media ID, and
// that media ID must resolve to the message ID the store uses (which is the LID form).
func TestMakeMediaID_OwnMessageInLIDChat(t *testing.T) {
	const msgID = "AC28AEBF88AA53C41EBECB3C98A4682B"
	const ownPhone = networkid.UserLoginID("989911912628")
	chat := types.NewJID("190375230701805", types.HiddenUserServer)
	ownLID := types.NewJID("224249537118341", types.HiddenUserServer)
	ownPN := types.NewJID(string(ownPhone), types.DefaultUserServer)

	makeInfo := func(sender, altSender types.JID) *types.MessageInfo {
		return &types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:      chat,
				Sender:    sender,
				SenderAlt: altSender,
				IsFromMe:  true,
			},
			ID: msgID,
		}
	}
	live := makeInfo(ownLID, ownPN)
	backfilled := makeInfo(ownPN, ownLID)

	// This is what the message store writes for both paths.
	wantMessageID := MakeMessageIDWithAltSender(chat, ownPN, ownLID, msgID)
	if got := MakeMessageIDWithAltSender(chat, ownLID, ownPN, msgID); got != wantMessageID {
		t.Fatalf("message ID mismatch between paths: live %q, backfill %q", got, wantMessageID)
	}

	liveMediaID := MakeMediaID(live, "", ownPhone, nil)
	backfilledMediaID := MakeMediaID(backfilled, "", ownPhone, nil)
	if !bytes.Equal(liveMediaID, backfilledMediaID) {
		t.Errorf("media IDs differ between paths:\n live     %x\n backfill %x", liveMediaID, backfilledMediaID)
	}

	for name, mediaID := range map[string]networkid.MediaID{"live": liveMediaID, "backfill": backfilledMediaID} {
		parsed, err := ParseMediaID(mediaID)
		if err != nil {
			t.Fatalf("%s: failed to parse media ID: %v", name, err)
		}
		if parsed.UserLogin != ownPhone {
			t.Errorf("%s: user login is %q, want %q", name, parsed.UserLogin, ownPhone)
		}
		if got := parsed.Message.String(); got != wantMessageID {
			t.Errorf("%s: media ID resolves to message %q, want %q", name, got, wantMessageID)
		}
	}
}

// Group chats are not LID-addressed, so the sender must be left exactly as received.
func TestMakeMediaID_GroupSenderNotRewritten(t *testing.T) {
	const msgID = "3A0D4F1C9B2E5A7D"
	chat := types.NewJID("120363000000000000", types.GroupServer)
	senderPN := types.NewJID("123456789", types.DefaultUserServer)
	senderLID := types.NewJID("987654321012345", types.HiddenUserServer)
	info := &types.MessageInfo{
		MessageSource: types.MessageSource{Chat: chat, Sender: senderPN, SenderAlt: senderLID},
		ID:            msgID,
	}

	parsed, err := ParseMediaID(MakeMediaID(info, "", "989911912628", nil))
	if err != nil {
		t.Fatalf("failed to parse media ID: %v", err)
	}
	want := MakeMessageIDWithAltSender(chat, senderPN, senderLID, msgID)
	if got := parsed.Message.String(); got != want {
		t.Errorf("media ID resolves to message %q, want %q", got, want)
	}
}
