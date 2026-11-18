from py_phone_caller_utils.py_phone_caller_db.py_phone_caller_piccolo_app.tables import (
    Calls,
)


async def select_to_recall(
    times_to_dial, lesser_seconds_to_forget, greater_sleep_and_retry
):
    """
    Selects call records that are eligible for recall based on dial attempts and timing criteria.

    This asynchronous function queries the Calls table for records that have not reached the maximum dial attempts,
    are within the specified time window, and have not completed their call cycle.

    Args:
        times_to_dial (int): Deprecated; the per-row ``times_to_dial`` column
            governs eligibility so the recall scan can never exceed the cap
            stored when the call was registered.
        lesser_seconds_to_forget (datetime): The lower bound of the time window for first dial.
        greater_sleep_and_retry (datetime): The upper bound of the time window for first dial.

    Returns:
        list: A list of call records matching the recall criteria.
    """

    result = await Calls.select(
        Calls.phone, Calls.message, Calls.seconds_to_forget
    ).where(
        # Compare against the cap stored on the row itself: the registration
        # path never increments past it, so a global value above the row cap
        # used to select the same call forever (dial loop every scan cycle).
        (Calls.dialed_times < Calls.times_to_dial)
        & (Calls.first_dial >= lesser_seconds_to_forget)
        & (Calls.first_dial <= greater_sleep_and_retry)
        & (Calls.cycle_done == False)
    )
    return result


async def select_backup_calls(max_backup_calls):
    """
    Selects call records that are eligible for backup recall.

    A call is eligible for backup when:
    - It has not been acknowledged
    - The main call's retry window (first_dial + seconds_to_forget) has expired
    - The backup call count is below the maximum allowed
    - The call cycle is not marked as done
    - The call was originally an on-call request

    Args:
        max_backup_calls (int): Maximum number of backup calls allowed per record.

    Returns:
        list: A list of call records matching the backup criteria.
    """
    # Select calls where the main retry window has expired (first_dial + seconds_to_forget < now)
    # We use raw SQL to compare first_dial + seconds_to_forget interval with current time
    # Note: Using string formatting for the integer parameter as Piccolo's raw() doesn't
    # support $1 style placeholders. The value comes from configuration, not user input.
    result = await Calls.raw(
        f"""
        SELECT id, phone, message, call_backup_callee_number_calls, seconds_to_forget
        FROM calls
        WHERE acknowledge_at = '-infinity'
          AND (first_dial + (seconds_to_forget || ' seconds')::interval) < NOW()
          AND call_backup_callee_number_calls < {int(max_backup_calls)}
          AND cycle_done = FALSE
          AND oncall = TRUE
        """
    )
    return result


async def increment_backup_call_count(call_id):
    """
    Increments the call_backup_callee_number_calls for a specific call record.

    Args:
        call_id (uuid): The ID of the call record to update.
    """
    await Calls.update(
        {
            Calls.call_backup_callee_number_calls: Calls.call_backup_callee_number_calls
            + 1
        }
    ).where(Calls.id == call_id)
