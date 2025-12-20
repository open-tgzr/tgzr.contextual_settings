from typing import Optional, Callable, List, Dict, Any, Union, Type

from nicegui import ui
from nicegui.elements.mixins.value_element import ValueElement
from pydantic import BaseModel

TYPE_TO_INPUT_UI_MAP = {
    int: ui.number,
    bool: ui.switch,
    str: ui.input,
    float: ui.number,
    List[int]: ui.input,
    List[str]: ui.input,
    list: ui.input,
    type(None): ui.input,
}

NAME_TO_INPUT_UI_MAP = {
    "ConstrainedIntValue": ui.number,
}


def match_type_to_input(in_type: Any) -> ValueElement:
    """
    Check using multiple methods which UI element matches the input.
    Pydantic Models are handled by form_from_pydantic recursively.
    :type in_type: The output of `type()` or the `type_` property of a Pydantic Field
    """
    try:
        return TYPE_TO_INPUT_UI_MAP[in_type]
    except KeyError:
        pass

    try:
        return NAME_TO_INPUT_UI_MAP[in_type.__name__]
    except KeyError:
        pass
    except AttributeError as ae:
        if (
            "__name__" in ae
        ):  # the particular object does not have a '__name__' attribute, so we try other things
            pass
        else:
            raise

    raise KeyError(f"Field of type: {in_type} is not supported")


def single_form_field(
    title: str, input_type: Any, default_value: Optional[Any] = None
) -> Callable:
    """accepts the basics of a form and produces a function that can be used to collect the value later"""
    ui_element = match_type_to_input(
        input_type
    )  # BaseModel will trigger an exception that can be caught above
    with ui.row().classes("items-center h-10"):
        ui.label(title).classes("text-h10 text-black")
        input_handle = ui_element().props("filled borderless dense")

    if default_value is not None:
        input_handle.value = default_value

    def get_value():
        return input_handle.value

    return get_value


def form_from_pydantic(
    model: Optional[Union[BaseModel, Type[BaseModel]]], value_getters: dict
) -> Dict["str", Callable]:
    """Creates a form from a pydantic model or instance"""
    if model is None:
        return value_getters

    elif isinstance(model, BaseModel):  # an instance
        title = model.__repr_name__()
        fields_info = [
            ((f_name, type(f_val), f_val), f_val)
            for f_name, f_val in model.__dict__.items()
        ]

    elif issubclass(model, BaseModel):  # a model
        title = model.__name__
        fields_info = [
            ((f_name, f_field.type_, f_field.default), f_field.type_)
            for f_name, f_field in model.__fields__.items()
        ]
    else:
        raise ValueError(f"Unsupported type. Input model is of type {type(model)}.")

    ui.label(title).classes("text-bold")
    for info in fields_info:
        if issubclass(info[0][1], BaseModel):  # check type
            value_getters |= {
                info[0][0]: form_from_pydantic(info[1], {})
            }  # [1] is dedicated for this case
        else:
            value_getters |= {info[0][0]: single_form_field(*info[0])}

    ui.separator()
    return value_getters


def collect_values_getters(value_getters: Dict[str, Callable]) -> Dict[str, Any]:
    """collects the values filled in the test data json form and returns a dict that can be used to create an instance of the model"""

    return {
        f_name: (
            collect_values_getters(f_getter) if type(f_getter) == dict else f_getter()
        )
        for f_name, f_getter in value_getters.items()
    }
