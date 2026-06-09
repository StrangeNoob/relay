// Persistence gate for the first-visit help overlay. Kept pure (takes the storage
// as a parameter) so it is trivially unit-testable and never throws on a blocked
// localStorage (private mode).

export const HELP_SEEN_KEY = "relay.help.seen";

// shouldAutoOpen reports whether the overlay should auto-open. It opens when the
// "seen" flag is absent; if storage access throws (privacy mode), it defaults to
// showing the overlay rather than failing.
export function shouldAutoOpen(storage: Pick<Storage, "getItem">): boolean {
  try {
    return storage.getItem(HELP_SEEN_KEY) === null;
  } catch {
    return true;
  }
}

// markSeen records that the visitor has dismissed the overlay. A blocked storage
// is swallowed so dismissing never throws.
export function markSeen(storage: Pick<Storage, "setItem">): void {
  try {
    storage.setItem(HELP_SEEN_KEY, "1");
  } catch {
    // ignore (private mode / storage disabled)
  }
}
