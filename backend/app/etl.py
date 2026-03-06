"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


import httpx


async def fetch_items() -> list[dict]:
    url = f"{settings.autochecker_api_url}/api/items"

    async with httpx.AsyncClient() as client:
        response = await client.get(
            url,
            auth=(settings.autochecker_email, settings.autochecker_password),
        )

    if response.status_code != 200:
        raise Exception(f"Failed to fetch items: {response.text}")

    return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    Uses pagination to fetch all logs in batches of 500.
    If `since` is provided, only fetch logs submitted after that timestamp.
    """
    all_logs: list[dict] = []
    url = f"{settings.autochecker_api_url}/api/logs"
    current_since = since

    while True:
        params: dict[str, str | int] = {"limit": 500}
        if current_since:
            params["since"] = current_since.isoformat()

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )

        if response.status_code != 200:
            raise Exception(f"Failed to fetch logs: {response.text}")

        data = response.json()
        logs = data.get("logs", [])
        all_logs.extend(logs)

        # Check if there are more pages
        if not data.get("has_more", False):
            break

        # Use the last log's submitted_at as the new since value
        if logs:
            last_log = logs[-1]
            current_since = datetime.fromisoformat(last_log["submitted_at"])
        else:
            break

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    Returns the number of newly created items.
    Maps labs by their short ID so tasks can find their parent.
    """
    from sqlmodel import select
    from app.models.item import ItemRecord

    new_count = 0
    lab_short_id_to_record: dict[str, ItemRecord] = {}

    # Process labs first
    for item in items:
        if item.get("type") != "lab":
            continue

        lab_title = item.get("title", "")
        lab_short_id = item.get("lab", "")

        # Check if lab already exists
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == lab_title,
            )
        )
        lab_record = existing.first()

        if lab_record is None:
            # Create new lab record
            lab_record = ItemRecord(type="lab", title=lab_title)
            session.add(lab_record)
            new_count += 1
            await session.flush()  # Get the ID

        # Map short ID to record for task lookup
        if lab_short_id:
            lab_short_id_to_record[lab_short_id] = lab_record

    # Process tasks
    for item in items:
        if item.get("type") != "task":
            continue

        task_title = item.get("title", "")
        lab_short_id = item.get("lab", "")

        # Find parent lab using short ID mapping
        parent_lab = lab_short_id_to_record.get(lab_short_id)
        if parent_lab is None:
            continue  # Skip task if parent lab not found

        # Check if task already exists with this title and parent_id
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == task_title,
                ItemRecord.parent_id == parent_lab.id,
            )
        )
        task_record = existing.first()

        if task_record is None:
            # Create new task record
            task_record = ItemRecord(
                type="task", title=task_title, parent_id=parent_lab.id
            )
            session.add(task_record)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    Returns the number of newly created interactions.
    Uses external_id for idempotent upserts (skip if exists).
    """
    from sqlmodel import select
    from app.models.learner import Learner
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord

    new_count = 0

    # Build lookup: (lab_short_id, task_short_id) -> item title
    # For labs: (lab, None) -> title
    # For tasks: (lab, task) -> title
    short_id_to_title: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        item_type = item.get("type")
        lab_short_id = item.get("lab", "")
        task_short_id = item.get("task")  # May be None for labs

        if item_type == "lab":
            key = (lab_short_id, None)
        else:  # task
            key = (lab_short_id, task_short_id)

        short_id_to_title[key] = item.get("title", "")

    for log in logs:
        # 1. Find or create Learner by external_id
        student_id = log.get("student_id", "")
        group = log.get("group", "")

        learner_result = await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        )
        learner = learner_result.first()

        if learner is None:
            learner = Learner(external_id=student_id, student_group=group)
            session.add(learner)
            await session.flush()

        # 2. Find matching item in the database
        lab_short_id = log.get("lab", "")
        task_short_id = log.get("task")  # May be None

        # Get title from lookup
        item_title = short_id_to_title.get((lab_short_id, task_short_id))
        if item_title is None:
            continue  # Skip if no matching item found

        # Query DB for ItemRecord with that title
        item_result = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item = item_result.first()

        if item is None:
            continue  # Skip if no matching item in DB

        # 3. Check if InteractionLog with this external_id already exists
        log_external_id = log.get("id")
        existing_result = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log_external_id)
        )
        existing_interaction = existing_result.first()

        if existing_interaction is not None:
            continue  # Skip if already exists (idempotent)

        # 4. Create InteractionLog
        submitted_at_str = log.get("submitted_at")
        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=datetime.fromisoformat(submitted_at_str)
            if submitted_at_str
            else datetime.now(),
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    Returns a dict: {"new_records": <number of new interactions>,
                     "total_records": <total interactions in DB>}
    """
    from sqlmodel import select, func
    from app.models.interaction import InteractionLog

    # Step 1: Fetch items from the API and load them into the database
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Determine the last synced timestamp
    # Query the most recent created_at from InteractionLog
    latest_result = await session.exec(
        select(InteractionLog.created_at)
        .order_by(
            InteractionLog.created_at.desc()  # type: ignore
        )
        .limit(1)
    )
    latest = latest_result.first()

    since = latest  # If no records exist, since=None (fetch everything)

    # Step 3: Fetch logs since that timestamp and load them
    # Pass the raw items list to load_logs so it can map short IDs to titles
    logs = await fetch_logs(since)
    new_records = await load_logs(logs, items, session)

    # Get total count of interactions in DB
    total_result = await session.exec(select(func.count(InteractionLog.id)))
    total = total_result.first()

    return {"new_records": new_records, "total_records": total or 0}
