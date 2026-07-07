package main

type Access int

const (
	AccessNone       Access = 0
	AccessRead       Access = 1
	AccessReadAppend Access = 2
	AccessReadWrite  Access = 3
)

func ParseAccess(s string) Access {
	switch s {
	case "r", "read-only":
		return AccessRead
	case "ra", "read-append":
		return AccessReadAppend
	case "rw", "read-write":
		return AccessReadWrite
	default:
		return AccessNone
	}
}
