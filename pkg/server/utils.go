package server

import "time"

func toExpiry(expires int) (expiry int) {
	if expires < 0 {
		expiry = -1
	} else {
		expiry = expires + int(time.Now().Unix())
	}
	return
}

func toExpires(expiry int) (expires int) {
	if expiry < 0 {
		expires = -1
	} else {
		expires = expiry - int(time.Now().Unix())
		if expires < 0 {
			expires = 0
		}
	}
	return
}
