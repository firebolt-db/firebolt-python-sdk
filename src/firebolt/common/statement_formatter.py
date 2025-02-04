from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Sequence, Union

from sqlparse import parse as parse_sql  # type: ignore
from sqlparse.sql import (  # type: ignore
    Comment,
    Comparison,
    Statement,
    Token,
    TokenList,
)
from sqlparse.tokens import Comparison as ComparisonType  # type: ignore
from sqlparse.tokens import Newline  # type: ignore
from sqlparse.tokens import Whitespace  # type: ignore
from sqlparse.tokens import Token as TokenType  # type: ignore

from firebolt.common._types import ParameterType, SetParameter
from firebolt.utils.exception import (
    DataError,
    InterfaceError,
    NotSupportedError,
)

escape_chars_v2 = {
    "\0": "\\0",
    "'": "''",
}

escape_chars_v1 = {
    "\0": "\\0",
    "'": "''",
    "\\": "\\\\",
}


class StatementFormatter:
    def __init__(self, escape_chars: Dict[str, str]):
        self.escape_chars = escape_chars

    def format_value(self, value: ParameterType) -> str:
        """For Python value to be used in a SQL query."""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float, Decimal)):
            return str(value)
        elif isinstance(value, str):
            return f"'{''.join(self.escape_chars.get(c, c) for c in value)}'"
        elif isinstance(value, datetime):
            if value.tzinfo is not None:
                value = value.astimezone(timezone.utc)
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(value, date):
            return f"'{value.isoformat()}'"
        elif isinstance(value, bytes):
            # Encode each byte into hex
            return "E'" + "".join(f"\\x{b:02x}" for b in value) + "'"
        if value is None:
            return "NULL"
        elif isinstance(value, Sequence):
            return f"[{', '.join(self.format_value(it) for it in value)}]"

        raise DataError(f"unsupported parameter type {type(value)}")

    def format_statement(
        self, statement: Statement, parameters: Sequence[ParameterType]
    ) -> str:
        """
        Substitute placeholders in a `sqlparse` statement with provided values.
        """
        idx = 0

        def process_token(token: Token) -> Token:
            nonlocal idx
            if token.ttype == TokenType.Name.Placeholder:
                # Replace placeholder with formatted parameter
                if idx >= len(parameters):
                    raise DataError(
                        "not enough parameters provided for substitution: given "
                        f"{len(parameters)}, found one more"
                    )
                formatted = self.format_value(parameters[idx])
                idx += 1
                return Token(TokenType.Text, formatted)
            if isinstance(token, TokenList):
                # Process all children tokens

                return TokenList([process_token(t) for t in token.tokens])
            return token

        formatted_sql = self.statement_to_sql(process_token(statement))

        if idx < len(parameters):
            raise DataError(
                "too many parameters provided for substitution:"
                f" given {len(parameters)}, "
                f"used only {idx}"
            )

        return formatted_sql

    def statement_to_set(self, statement: Statement) -> Optional[SetParameter]:
        """
        Try to parse `statement` as a `SET` command.
        Return `None` if it's not a `SET` command.
        """
        # Filter out meaningless tokens like Punctuation and Whitespaces
        skip_types = [Whitespace, Newline]
        tokens = [
            token
            for token in statement.tokens
            if token.ttype not in skip_types and not isinstance(token, Comment)
        ]
        # Trim tail punctuation
        right_idx = len(tokens) - 1
        while str(tokens[right_idx]) == ";":
            right_idx -= 1

        tokens = tokens[: right_idx + 1]

        # Check if it's a SET statement by checking if it starts with set
        if (
            len(tokens) > 0
            and tokens[0].ttype == TokenType.Keyword
            and tokens[0].value.lower() == "set"
        ):
            # Check if set statement has a valid format
            if len(tokens) == 2 and isinstance(tokens[1], Comparison):
                return SetParameter(
                    self.statement_to_sql(tokens[1].left),
                    self.statement_to_sql(tokens[1].right).strip("'"),
                )
            # Or if at least there is a comparison
            cmp_idx = next(
                (
                    i
                    for i, token in enumerate(tokens)
                    if token.ttype == ComparisonType or isinstance(token, Comparison)
                ),
                None,
            )
            if cmp_idx:
                left_tokens, right_tokens = tokens[1:cmp_idx], tokens[cmp_idx + 1 :]
                if isinstance(tokens[cmp_idx], Comparison):
                    left_tokens = left_tokens + [tokens[cmp_idx].left]
                    right_tokens = [tokens[cmp_idx].right] + right_tokens

                if left_tokens and right_tokens:
                    return SetParameter(
                        "".join(self.statement_to_sql(t) for t in left_tokens),
                        "".join(self.statement_to_sql(t) for t in right_tokens).strip(
                            "'"
                        ),
                    )

            raise InterfaceError(
                f"Invalid set statement format: {self.statement_to_sql(statement)},"
                " expected SET <param> = <value>"
            )
        return None

    def statement_to_sql(self, statement: Statement) -> str:
        return str(statement).strip().rstrip(";")

    def split_format_sql(
        self, query: str, parameters: Sequence[Sequence[ParameterType]]
    ) -> List[Union[str, SetParameter]]:
        """
        Multi-statement query formatting will result in `NotSupportedError`.
        Instead, split a query into a separate statement and format with parameters.
        """
        statements = parse_sql(query)
        if not statements:
            return [query]

        if parameters:
            if len(statements) > 1:
                raise NotSupportedError(
                    "Formatting multi-statement queries is not supported."
                )
            if self.statement_to_set(statements[0]):
                raise NotSupportedError("Formatting set statements is not supported.")
            return [
                self.format_statement(statements[0], paramset)
                for paramset in parameters
            ]

        # Try parsing each statement as a SET, otherwise return as a plain sql string
        return [
            self.statement_to_set(st) or self.statement_to_sql(st) for st in statements
        ]


def create_statement_formatter(version: int) -> StatementFormatter:
    if version == 1:
        return StatementFormatter(escape_chars_v1)
    elif version == 2:
        return StatementFormatter(escape_chars_v2)
    else:
        raise ValueError(f"Unsupported version: {version}")
