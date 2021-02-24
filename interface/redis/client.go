package redis

type Connection interface {
	Write([]byte) error

	SubChanel(channel string)
	UnSubsChannel(channel string)
	SubsCount()int
	GetChannels()[]string
}
