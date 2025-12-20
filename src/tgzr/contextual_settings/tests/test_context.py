from tgzr.contextual_settings.context_name import (
    expand_context_names,
    set_environ_getter,
)


def test_context_names() -> None:

    def get_env():
        return dict(
            PROJECT="MyProject",
            USER="Alice",
            START="100",
            END="150",
            RANGE="$START-$END",
        )

    set_environ_getter(get_env)

    tests = [
        (
            # env vars
            ["STUDIO", "$PROJECT", "$USER"],
            ["STUDIO", "MyProject", "Alice"],
        ),
        (
            # nested env var
            ["$RANGE"],
            ["100-150"],
        ),
        (
            # env var + path
            ["Studio", "$PROJECT", "PROD", "[Assets/Props/Glass]", "Animation"],
            [
                "Studio",
                "MyProject",
                "PROD",
                "Assets",
                "Assets/Props",
                "Assets/Props/Glass",
                "Animation",
            ],
        ),
        (
            # abs path
            ["[/abs/entity/path]", "DEV"],
            ["abs", "abs/entity", "abs/entity/path", "DEV"],
        ),
        (
            # un-normed path
            ["[/abs////entity/path/]", "DEV"],
            ["abs", "abs/entity", "abs/entity/path", "DEV"],
        ),
    ]

    for value, result in tests:
        reduced = expand_context_names(value)
        # print("---->", value)
        # print("     ", result)
        # print("     ", reduced)
        assert reduced == result


def test_context() -> None:
    test_context_names()
