import functools
from karppipeline.common import PipelineException
from karppipeline.models import InferredField


def to_int_update_schema(field: InferredField) -> InferredField:
    field.type = "integer"
    return field


@functools.cache
def to_int(_, val: str) -> int:
    try:
        return int(val)
    except ValueError:
        raise PipelineException(f"util:to_int failed on value: {val}")
