package protocol

import "testing"

func TestTypeConstantValues(t *testing.T) {
	cases := []struct {
		got  MsgType
		want uint8
		name string
	}{
		{HELLO, 0x01, "HELLO"},
		{WELCOME, 0x02, "WELCOME"},
		{PING, 0x03, "PING"},
		{PONG, 0x04, "PONG"},
		{SEND, 0x10, "SEND"},
		{REQUEST, 0x11, "REQUEST"},
		{RESPONSE, 0x12, "RESPONSE"},
		{CANCEL, 0x13, "CANCEL"},
		{ACK, 0x1E, "ACK"},
		{NACK, 0x1F, "NACK"},
		{STREAM_OPEN, 0x20, "STREAM_OPEN"},
		{STREAM_DATA, 0x21, "STREAM_DATA"},
		{STREAM_END, 0x22, "STREAM_END"},
		{SUBSCRIBE, 0x30, "SUBSCRIBE"},
		{SUBACK, 0x31, "SUBACK"},
		{UNSUB, 0x32, "UNSUB"},
		{PUBLISH, 0x33, "PUBLISH"},
		{TICKET_REQ, 0x40, "TICKET_REQ"},
		{TICKET, 0x41, "TICKET"},
		{P2P_HELLO, 0x42, "P2P_HELLO"},
		{ERROR, 0xFF, "ERROR"},
	}
	for _, c := range cases {
		if uint8(c.got) != c.want {
			t.Errorf("%s = 0x%02X, want 0x%02X", c.name, uint8(c.got), c.want)
		}
		if got := TypeName(c.got); got != c.name {
			t.Errorf("TypeName(0x%02X) = %q, want %q", c.want, got, c.name)
		}
	}
}

func TestTypeNameUnknown(t *testing.T) {
	if got := TypeName(MsgType(0x99)); got == "" {
		t.Error("TypeName of unknown type should return a non-empty readable name")
	}
}
