from __future__ import annotations

from typing import Type, TypeVar, Any, get_args

from uuid import uuid4, UUID

import pydantic


class Item(pydantic.BaseModel):
    """
    An Item is a model which can be stored in a Collection.
    All fields MUST have a default value.
    """

    uid: UUID | str = pydantic.Field(default_factory=uuid4)
    name: str = "NoName"
    parent: Item | None = None

    def pk(self):
        """
        Returns the value to use as an unic identifier in its
        collection.
        Default is to use `self.uid` but you can subclass
        and use `self.name` if you are sure it's unic.
        """
        return str(self.uid)


class NamedItem(Item):
    """
    An Item which uses its name as unic indentifier in its
    collection.
    """

    uid: UUID | str = pydantic.Field(default_factory=uuid4)
    name: str = "NoName"
    parent: Item | None = None

    def pk(self):
        """
        Returns the value to use as an unic identifier.
        Default is to use `self.uid` but you can subclass
        and use `self.name` if you are sure it's uniq.
        """
        return self.name


ItemType = TypeVar("ItemType", bound=Item)

T = TypeVar("T")


class Collection[ItemType: Item](pydantic.BaseModel):
    items: list[ItemType] = pydantic.Field(default_factory=list)

    @classmethod
    def Field(cls, item_type: Type[ItemType]) -> Any:
        return pydantic.Field(default_factory=Collection[item_type])

    def __class_getitem__(
        cls: Type[T], params: Type[Any] | tuple[Type[Any], ...]
    ) -> Type[Any]:
        """Hack to have __orig_class__ working on pydandic.BaseModel"""
        create_model = super().__class_getitem__(params)

        class _Generic[T]:
            pass

        create_model.__orig_class__ = _Generic[params]  # type: ignore
        return create_model

    def item_type(self) -> Type[ItemType]:
        return get_args(self.__orig_class__)[0]

    @pydantic.field_serializer("items", when_used="unless-none")
    def dump_items(self, items: list[ItemType]):
        """
        This turns the `items:list[ItemType]` into {'@keys':list[pk], '@':dict[pk, ItemType], '@defaults':...}
        """
        defaults = self.item_type()().model_dump()
        d = {
            "@keys": [i.pk() for i in items],
            "@": dict([(i.pk(), i.model_dump()) for i in items]),
            "@defaults": defaults,
        }
        return d

    def update(self, name: str, **values):
        existing = self.get_by(name=name)
        if not existing:
            raise ValueError(f"No item with name={name} found in {self}")
        for k, v in values.items():
            setattr(existing, k, v)

    def add_or_update(self, name: str, **values) -> ItemType:
        existing = self.get_by(name=name)
        if existing is not None:
            for k, v in values.items():
                setattr(existing, k, v)
            return existing
        else:
            return self.add(self.item_type(), name, **values)

    def add(self, item_type: Type[ItemType], name: str, **values) -> ItemType:
        uid = values.pop("uid", uuid4())  # FIXME: this does not work with NamedItem !
        item = item_type(uid=uid, name=name, **values)
        self.items.append(item)
        return item

    def remove(self, uid: UUID) -> ItemType:
        for item in self.items:
            if item.uid == uid:
                self.items.remove(item)
                return item
        raise ValueError(f"Not item with {uid=}")

    def clear(self):
        self.items[:] = []

    def get_by(self, **attrs) -> ItemType | None:
        """
        team.members.get_by(gender='fluid', age=42)
        """
        for item in self.items:
            matches = False
            for k, v in attrs.items():
                # exception for uid:
                # since it can be an uuid or a str, we only compare
                # the str form
                if k == "uid":
                    v = str(v)
                    this_one = str(getattr(item, k))
                else:
                    this_one = getattr(item, k)
                if this_one != v:
                    matches = False
                    continue
                matches = True
            if matches:
                return item
        return None
