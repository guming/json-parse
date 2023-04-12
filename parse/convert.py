from typing import Any
from runtime_value import RuntimeValue

def convert_to_runtime(data: Any) -> RuntimeValue:
    return RuntimeValue.of(data)
def convert_to_python(data: RuntimeValue) -> Any:
    return data.to_python()