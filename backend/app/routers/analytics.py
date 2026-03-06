from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select
from sqlalchemy import func, case

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


def normalize_lab(lab: str) -> str:
    # "lab-04" -> "Lab 04"
    return lab.replace("lab-", "Lab ")


async def get_lab_tasks(session: AsyncSession, lab: str):
    lab_title = normalize_lab(lab)

    lab_item = (
        await session.exec(
            select(ItemRecord).where(ItemRecord.title.contains(lab_title))
        )
    ).first()

    if not lab_item:
        return []

    tasks = (
        await session.exec(
            select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
        )
    ).all()

    return [t.id for t in tasks]


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):

    task_ids = await get_lab_tasks(session, lab)

    bucket = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    )

    query = (
        select(bucket.label("bucket"), func.count())
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(bucket)
    )

    rows = (await session.exec(query)).all()

    result = {
        "0-25": 0,
        "26-50": 0,
        "51-75": 0,
        "76-100": 0,
    }

    for bucket_name, count in rows:
        result[bucket_name] = count

    return [{"bucket": k, "count": v} for k, v in result.items()]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):

    task_ids = await get_lab_tasks(session, lab)

    query = (
        select(
            ItemRecord.title,
            func.round(func.avg(InteractionLog.score), 1),
            func.count(InteractionLog.id),
        )
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.id.in_(task_ids))
        .group_by(ItemRecord.id)
        .order_by(ItemRecord.title)
    )

    rows = (await session.exec(query)).all()

    return [
        {
            "task": title,
            "avg_score": avg_score,
            "attempts": attempts,
        }
        for title, avg_score, attempts in rows
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):

    task_ids = await get_lab_tasks(session, lab)

    query = (
        select(
            func.date(InteractionLog.created_at),
            func.count(),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    rows = (await session.exec(query)).all()

    return [
        {
            "date": str(date),
            "submissions": count,
        }
        for date, count in rows
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):

    task_ids = await get_lab_tasks(session, lab)

    query = (
        select(
            Learner.student_group,
            func.round(func.avg(InteractionLog.score), 1),
            func.count(func.distinct(Learner.id)),
        )
        .join(InteractionLog, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    rows = (await session.exec(query)).all()

    return [
        {
            "group": group,
            "avg_score": avg_score,
            "students": students,
        }
        for group, avg_score, students in rows
    ]