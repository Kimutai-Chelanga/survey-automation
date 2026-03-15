import random
import uuid
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def _round_to_nearest_second(min_ms: int, max_ms: int) -> int:
    """
    Pick a random value between min_ms and max_ms, rounded to the nearest 1000ms.

    Examples:
        min=0,     max=10000  → one of  0, 1000, 2000, ..., 10000
        min=10000, max=20000  → one of  10000, 11000, ..., 20000
        min=0,     max=0      → 0  (both zero → return 0)

    The range is converted to whole-second steps, then a step is chosen
    uniformly at random so every second boundary has equal probability.
    """
    # Clamp to multiples of 1000
    min_s = int(min_ms) // 1000   # floor to whole seconds
    max_s = int(max_ms) // 1000   # floor to whole seconds

    if min_s >= max_s:
        return min_s * 1000

    chosen_s = random.randint(min_s, max_s)
    return chosen_s * 1000


def resolve_timing_ms(timing_config: dict, default_min: int = 0, default_max: int = 10000) -> int:
    """
    Resolve the actual millisecond value to use for a timing config dict.

    Supports two modes:
        'fixed'  → always return max_milliseconds exactly (no randomisation)
        'range'  → pick a random whole-second value between min and max

    Args:
        timing_config: dict with keys 'mode', 'min_milliseconds', 'max_milliseconds'
        default_min: fallback minimum if keys are absent
        default_max: fallback maximum if keys are absent

    Returns:
        int: milliseconds to use
    """
    mode    = timing_config.get('mode', 'range')
    min_ms  = int(timing_config.get('min_milliseconds', default_min))
    max_ms  = int(timing_config.get('max_milliseconds', default_max))

    if mode == 'fixed':
        # Fixed mode: use exactly max_milliseconds (rounded to nearest second)
        fixed_ms = round(max_ms / 1000) * 1000
        logger.debug(f"   Timing mode=fixed → {fixed_ms}ms")
        return fixed_ms
    else:
        # Range mode: random whole-second value between min and max
        value_ms = _round_to_nearest_second(min_ms, max_ms)
        logger.debug(f"   Timing mode=range [{min_ms}–{max_ms}ms] → {value_ms}ms")
        return value_ms


def apply_human_like_timing(workflow_data, automa_config):
    """
    Apply timing to workflow blocks AFTER content has been added.
    Respects 'fixed' vs 'range' mode for both press_keys and click_elements.

    Args:
        workflow_data: Complete workflow data with content already inserted
        automa_config: Configuration with press_keys, click_elements (including mode field)

    Returns:
        Updated workflow_data with timing applied
    """
    press_keys_config     = automa_config.get('press_keys', {})
    click_elements_config = automa_config.get('click_elements', {})

    press_mode = press_keys_config.get('mode', 'range')
    click_mode = click_elements_config.get('mode', 'range')

    logger.info("🎭 Applying timing (press_keys mode=%s, click_elements mode=%s)...", press_mode, click_mode)
    logger.info(f"   Press keys config:      {press_keys_config}")
    logger.info(f"   Click elements config:  {click_elements_config}")

    # Process blocks in drawflow structure
    if 'drawflow' in workflow_data and 'nodes' in workflow_data['drawflow']:
        for node in workflow_data['drawflow']['nodes']:
            if node.get('type') == 'BlockGroup' and 'data' in node and 'blocks' in node['data']:
                node['data']['blocks'] = randomize_block_timings(
                    node['data']['blocks'],
                    press_keys_config,
                    click_elements_config
                )
    elif 'blocks' in workflow_data:
        workflow_data['blocks'] = randomize_block_timings(
            workflow_data['blocks'],
            press_keys_config,
            click_elements_config
        )

    return workflow_data


def randomize_block_timings(blocks, press_keys_config, click_elements_config):
    """
    Apply timing to each block type.

    - press-key blocks:     respects press_keys_config mode.
                            In range mode there is a 10% chance of a longer pause
                            BUT it is always capped to max_milliseconds so it
                            never exceeds what the user configured.
    - click-element blocks: respects click_elements_config mode for timeout variation.

    All timing values are whole-second multiples (1000 ms steps).

    Args:
        blocks: List of workflow blocks
        press_keys_config:     {mode, min_milliseconds, max_milliseconds}
        click_elements_config: {mode, min_milliseconds, max_milliseconds}
    """
    press_mode = press_keys_config.get('mode', 'range')
    click_mode = click_elements_config.get('mode', 'range')

    # Read the configured ceiling once so we can enforce it everywhere
    press_max_ms = int(press_keys_config.get('max_milliseconds', 10000))

    for block in blocks:
        block_id   = block.get('id', '')
        block_data = block.get('data', {})

        # ── PRESS-KEY BLOCKS ────────────────────────────────────────────────
        if block_id == 'press-key':
            press_time_ms = resolve_timing_ms(press_keys_config, default_min=0, default_max=10000)

            if press_mode == 'range':
                # 10% chance of a slightly longer "thinking" pause — ONLY in range mode
                # and ALWAYS capped to the configured max so the user's setting is respected
                if random.random() < 0.1:
                    extended      = int(press_time_ms * random.uniform(1.5, 2.5))
                    extended      = round(extended / 1000) * 1000
                    press_time_ms = min(extended, press_max_ms)  # ← cap to configured max

            block_data['pressTime'] = str(press_time_ms)
            logger.debug(f"   Press-key (mode={press_mode}): {press_time_ms}ms")

        # ── CLICK-ELEMENT BLOCKS ────────────────────────────────────────────
        elif block_id in ('event-click', 'click-element'):
            if 'waitSelectorTimeout' in block_data:
                if click_mode == 'fixed':
                    # Fixed mode: use the configured fixed value as the timeout
                    fixed_ms    = resolve_timing_ms(click_elements_config, default_min=0, default_max=10000)
                    new_timeout = max(5000, fixed_ms)
                else:
                    # Range mode: add a whole-second variation (-2s to +3s) around the base
                    # but cap the result to the configured max
                    click_max_ms    = int(click_elements_config.get('max_milliseconds', 10000))
                    base_timeout    = block_data.get('waitSelectorTimeout', 15000)
                    variation_steps = random.randint(-2, 3)
                    new_timeout     = max(5000, base_timeout + variation_steps * 1000)
                    new_timeout     = min(new_timeout, max(click_max_ms, 5000))  # ← cap to configured max

                block_data['waitSelectorTimeout'] = new_timeout
                logger.debug(f"   Click element (mode={click_mode}): timeout {new_timeout}ms")

    return blocks


def add_typing_variations(blocks, press_keys_config=None):
    """
    Add realistic typing variations to press-key blocks:
    - Longer pauses at word boundaries
    - Faster typing mid-word
    - Occasional hesitation

    NOTE: Typing variations are ONLY applied in 'range' mode.
          In 'fixed' mode the press time is a deliberate constant and must not drift.

    ALL resulting values are capped to max_milliseconds so the user's configured
    ceiling is always respected regardless of variation multipliers.

    All press times remain whole-second multiples after adjustment.
    """
    # Skip all variations when press mode is 'fixed'
    press_mode = (press_keys_config or {}).get('mode', 'range')
    if press_mode == 'fixed':
        logger.debug("   Skipping typing variations (press_keys mode=fixed)")
        return blocks

    # Read the configured ceiling — variations must never exceed this
    press_max_ms = int((press_keys_config or {}).get('max_milliseconds', 10000))

    word_boundary_chars = [' ', '.', ',', '!', '?', '\n']
    consecutive_chars   = 0

    for block in blocks:
        if block.get('id') != 'press-key':
            continue

        keys           = block.get('data', {}).get('keys', '')
        current_ms_str = block.get('data', {}).get('pressTime', '1000')

        try:
            current_ms = int(current_ms_str)
        except (ValueError, TypeError):
            current_ms = 1000

        if keys in word_boundary_chars:
            # Longer pause at word boundaries — scale up then re-round, cap to max
            new_ms = round(current_ms * random.uniform(1.2, 1.8) / 1000) * 1000
            new_ms = min(max(1000, new_ms), press_max_ms)  # ← cap to configured max
            block['data']['pressTime'] = str(new_ms)
            consecutive_chars = 0
        else:
            consecutive_chars += 1

            if 4 <= consecutive_chars <= 8:
                # Faster typing in the middle of a word — scale down then re-round
                # (scaling down so no cap needed here, but apply floor of 0)
                new_ms = round(current_ms * random.uniform(0.7, 0.9) / 1000) * 1000
                block['data']['pressTime'] = str(max(0, new_ms))

            elif random.random() < 0.05:
                # Occasional hesitation — scale up then re-round, cap to max
                new_ms = round(current_ms * random.uniform(1.5, 2.0) / 1000) * 1000
                new_ms = min(max(1000, new_ms), press_max_ms)  # ← cap to configured max
                block['data']['pressTime'] = str(new_ms)

    return blocks


# ============================================================================
# INTEGRATION WITH WORKFLOW GENERATION
# ============================================================================

def finalize_workflow_timing(workflow_data, automa_config):
    """
    Final step: Apply all timing randomizations AFTER content has been inserted.
    Respects 'fixed' vs 'range' mode for press_keys and click_elements.

    IMPORTANT: In create_workflow_with_multi_content() this function is called
    BEFORE placeholder replacement so that character blocks (which don't exist
    yet) are never touched. The character blocks get their timing directly from
    resolve_timing_ms() inside generate_character_press_key_blocks().

    All variation logic (10% extension, word-boundary scaling, hesitation) is:
        - Skipped entirely in 'fixed' mode
        - Capped to max_milliseconds in 'range' mode so the user's configured
          ceiling is always respected

    Args:
        workflow_data: Complete workflow with content already inserted
        automa_config: Configuration dict with press_keys / click_elements
                       (each having 'mode', 'min_milliseconds', 'max_milliseconds')

    Returns:
        workflow_data with timing applied
    """
    press_mode = automa_config.get('press_keys', {}).get('mode', 'range')
    click_mode = automa_config.get('click_elements', {}).get('mode', 'range')
    logger.info(f"🎬 Finalizing workflow timing (press={press_mode}, click={click_mode})...")

    # 1. Apply base timing (press-key, click-element)
    workflow_data = apply_human_like_timing(workflow_data, automa_config)

    # 2. Add typing variations — only in range mode (skipped automatically if fixed)
    #    Variations are capped to max_milliseconds so user settings are respected
    press_keys_config = automa_config.get('press_keys', {})
    if 'drawflow' in workflow_data and 'nodes' in workflow_data['drawflow']:
        for node in workflow_data['drawflow']['nodes']:
            if node.get('type') == 'BlockGroup' and 'data' in node and 'blocks' in node['data']:
                node['data']['blocks'] = add_typing_variations(
                    node['data']['blocks'],
                    press_keys_config=press_keys_config
                )
    elif 'blocks' in workflow_data:
        workflow_data['blocks'] = add_typing_variations(
            workflow_data['blocks'],
            press_keys_config=press_keys_config
        )

    logger.info("✅ Workflow timing finalized")
    return workflow_data
