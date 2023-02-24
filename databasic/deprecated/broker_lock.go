package deprecated

import "fmt"

const (
	ROUTER    int = 1
	SCHEDULER int = 2
	CONTROLER int = 3
)

const (
	LOCK   int = 1
	UNLOCK int = 0
)

const (
	SUCCESSFUL int = 1
	FAILED     int = -1
)

const (
	UNLOCKED                          int = 0
	ROUTER_LOCKED                     int = 1
	SCHEDULER_LOCKED                  int = 2
	CONTROLER_LOCKED                  int = 3
	ROUTER_SCHEDULER_LOCKED           int = 4
	ROUTER_CONTROLER_LOCKED           int = 5
	SCHEDULER_CONTROLER_LOCKED        int = 6
	ROUTER_SCHEDULER_CONTROLER_LOCKED int = 7
)

var mux int

var critical chan int = make(chan int, 1)

func Lock_stat_switcher(role int, operation int) int {
	if role < 1 || role > 3 || operation > 1 || operation < 0 {
		return FAILED
	}

	switch mux {
	case UNLOCKED:
		if role == ROUTER && operation == LOCK {
			mux = ROUTER_LOCKED
			return SUCCESSFUL
		} else if role == SCHEDULER && operation == LOCK {
			mux = SCHEDULER_LOCKED
			return SUCCESSFUL
		} else if role == CONTROLER && operation == LOCK {
			mux = CONTROLER_LOCKED
			return SUCCESSFUL
		} else {
			return SUCCESSFUL
		}

	case ROUTER_LOCKED:
		if role == ROUTER && operation == UNLOCK {
			mux = UNLOCKED
			return SUCCESSFUL
		} else if role == ROUTER && operation == LOCK {
			return SUCCESSFUL
		} else {
			return FAILED
		}

	case SCHEDULER_LOCKED:
		if role == ROUTER && operation == LOCK {
			mux = ROUTER_SCHEDULER_LOCKED
			return SUCCESSFUL
		} else if role == SCHEDULER && operation == UNLOCK {
			mux = UNLOCKED
			return SUCCESSFUL
		} else if role == SCHEDULER && operation == LOCK {
			return SUCCESSFUL
		} else {
			return FAILED
		}

	case CONTROLER_LOCKED:
		if role == ROUTER && operation == LOCK {
			mux = ROUTER_CONTROLER_LOCKED
			return SUCCESSFUL
		} else if role == SCHEDULER && operation == LOCK {
			mux = SCHEDULER_CONTROLER_LOCKED
			return SUCCESSFUL
		} else if role == CONTROLER && operation == UNLOCK {
			mux = UNLOCKED
			return SUCCESSFUL
		} else if role == CONTROLER && operation == LOCK {
			return SUCCESSFUL
		} else {
			return FAILED
		}

	case ROUTER_SCHEDULER_LOCKED:
		if role == ROUTER && operation == UNLOCK {
			mux = SCHEDULER_LOCKED
			return SUCCESSFUL
		} else if role == ROUTER && operation == LOCK {
			return SUCCESSFUL
		} else {
			return FAILED
		}

	case ROUTER_CONTROLER_LOCKED:
		if role == ROUTER && operation == UNLOCK {
			mux = CONTROLER_LOCKED
			return SUCCESSFUL
		} else if role == ROUTER && operation == LOCK {
			return SUCCESSFUL
		} else {
			return FAILED
		}

	case SCHEDULER_CONTROLER_LOCKED:
		if role == ROUTER && operation == LOCK {
			mux = ROUTER_SCHEDULER_CONTROLER_LOCKED
			return SUCCESSFUL
		} else if role == SCHEDULER && operation == UNLOCK {
			mux = CONTROLER_LOCKED
			return SUCCESSFUL
		} else if role == SCHEDULER && operation == LOCK {
			return SUCCESSFUL
		} else {
			return FAILED
		}

	case ROUTER_SCHEDULER_CONTROLER_LOCKED:
		if role == ROUTER && operation == UNLOCK {
			mux = SCHEDULER_CONTROLER_LOCKED
			return SUCCESSFUL
		} else if role == ROUTER && operation == LOCK {
			return SUCCESSFUL
		} else {
			return FAILED
		}

	default:
		fmt.Printf("The mux state is error!\n\r")
		return FAILED
	}
}

func router_lock() int {
	ok := Lock_stat_switcher(ROUTER, LOCK)
	return ok
}

func router_unlock() int {
	ok := Lock_stat_switcher(ROUTER, UNLOCK)
	return ok
}

func scheduler_lock() int {
	ok := Lock_stat_switcher(SCHEDULER, LOCK)
	return ok
}

func scheduler_unlock() int {
	ok := Lock_stat_switcher(SCHEDULER, UNLOCK)
	return ok
}

func controler_lock() int {
	ok := Lock_stat_switcher(CONTROLER, LOCK)
	return ok
}

func controler_unlock() int {
	ok := Lock_stat_switcher(CONTROLER, UNLOCK)
	return ok
}

func Critical_enter() int {
	select {
	case <-critical:
		return SUCCESSFUL
	default:
		return FAILED
	}
}

func Critical_leave() {
	select {
	case critical <- 1:
		return
	default:
		fmt.Printf("You leave critical zone, while you don`t enter the critical zone!\n\r")
	}
}

func Critical_exit() int {
	select {
	case <-critical:
		critical <- 1
		return NO
	default:
		return YES
	}
}
