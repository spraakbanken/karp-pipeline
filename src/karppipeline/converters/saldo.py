from karppipeline.common import PipelineException
from karppipeline.models import InferredField


def id_to_baseform_update_schema(field: InferredField) -> InferredField:
    return field


def id_to_baseform(_, saldo_id: str) -> str:
    baseform = saldo_id.split("..")[0]
    if ".." in baseform:
        raise PipelineException("Failed to create baseform from Saldo ID")
    return baseform.replace("_", " ")
