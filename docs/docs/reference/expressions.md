---
sidebar_position: 2
title: Expressions
---

# Expressions

Expressions are used throughout defacto definitions: in guards, computed properties, field mappings, per-effect conditions, and identity normalization. They evaluate against event data and entity state to produce values.

## Syntax

### Field access

Expressions can reference event data and entity properties using dot notation.

```
event.email              # field from the normalized event
entity.mrr               # property on the current entity
event.user.address.city  # nested field (dot-path into JSON)
event.items.0.product_id # array indexing via dot-path
value                    # bare variable (used in identity normalize expressions)
```

Missing fields return `null` rather than raising an error.

### Literals

```
42                  # integer
3.14                # float
"hello"             # string (double quotes)
'hello'             # string (single quotes)
true / false        # boolean
null                # null
30d / 24h / 90m     # duration (days, hours, minutes, seconds)
```

### Operators

From lowest to highest precedence:

| Operator | Description |
|---|---|
| `or`, `\|\|` | Logical or (short-circuit) |
| `and`, `&&` | Logical and (short-circuit) |
| `==`, `!=` | Equality |
| `>`, `<`, `>=`, `<=` | Comparison |
| `+`, `-` | Addition, subtraction (also string concat, datetime arithmetic) |
| `*`, `/` | Multiplication, division |
| `**` | Exponentiation |
| `not`, `!` | Logical not (unary) |

### Conditionals

```
if event.amount > 100 then "high" else "low"
```

### Indexing

Bracket indexing works on any expression that returns an array.

```
split(event.email, "@")[1]    # domain part of an email
event.items[-1]               # last element (negative indexing)
```

Out-of-bounds access returns `null`.

### Type coercion

Arithmetic operations between `Int` and `Float` coerce to `Float`. String `+` String produces concatenation. DateTime `+` Duration produces a new DateTime. DateTime `-` DateTime produces a Duration.

## Functions

### String (12)

| Function | Signature | Returns | Description |
|---|---|---|---|
| `str::to_lowercase` | `(String)` | String | Convert to lowercase |
| `str::to_uppercase` | `(String)` | String | Convert to uppercase |
| `str::trim` | `(String)` | String | Remove leading and trailing whitespace |
| `str::substring` | `(String, Int, Int)` | String | Extract substring (start, length). Character-based, not byte-based |
| `starts_with` | `(String, String)` | Bool | Check if string starts with prefix |
| `ends_with` | `(String, String)` | Bool | Check if string ends with suffix |
| `contains` | `(String, String)` | Bool | Check if string contains substring |
| `contains` | `(Array, value)` | Bool | Check if array contains value |
| `replace` | `(String, String, String)` | String | Replace all occurrences of second arg with third |
| `regex_match` | `(String, String)` | Bool | Test if string matches regex pattern |
| `regex_extract` | `(String, String)` | String or null | Return first capture group, or null if no match |
| `to_string` | `(any)` | String | Convert any value to its string representation |
| `len` | `(String)` | Int | String length |
| `len` | `(Array)` | Int | Array length |

### Math (6)

| Function | Signature | Returns | Description |
|---|---|---|---|
| `math::abs` | `(Int or Float)` | same type | Absolute value |
| `floor` | `(Float)` | Int | Round down |
| `ceil` | `(Float)` | Int | Round up |
| `round` | `(Float)` | Int | Round to nearest integer |
| `min` | `(number, number)` | number | Smaller of two values. Mixed Int/Float coerces to Float |
| `max` | `(number, number)` | number | Larger of two values. Mixed Int/Float coerces to Float |

### Date/Time (13)

| Function | Signature | Returns | Description |
|---|---|---|---|
| `now` | `()` | DateTime | Current UTC time |
| `days_between` | `(DateTime, DateTime)` | Int | Absolute difference in days |
| `hours_between` | `(DateTime, DateTime)` | Int | Absolute difference in hours |
| `minutes_between` | `(DateTime, DateTime)` | Int | Absolute difference in minutes |
| `days_since` | `(DateTime)` | Int | Days elapsed since the given time |
| `year_of` | `(DateTime)` | Int | Year component |
| `month_of` | `(DateTime)` | Int | Month component (1-12) |
| `day_of` | `(DateTime)` | Int | Day of month component (1-31) |
| `hour_of` | `(DateTime)` | Int | Hour component (0-23) |
| `minute_of` | `(DateTime)` | Int | Minute component (0-59) |
| `day_of_week` | `(DateTime)` | Int | Day of week (0=Monday through 6=Sunday) |
| `date_trunc` | `(DateTime, String)` | DateTime | Truncate to unit: `"year"`, `"month"`, `"day"`, `"hour"`, `"minute"` |
| `parse_datetime` | `(String, String)` | DateTime | Parse string with strftime format. Assumes UTC |

### Array (2)

| Function | Signature | Returns | Description |
|---|---|---|---|
| `split` | `(String, String)` | Array | Split string by delimiter |
| `join` | `(Array, String)` | String | Join array elements with delimiter. Non-strings are coerced |

### Type casting (2)

| Function | Signature | Returns | Description |
|---|---|---|---|
| `to_int` | `(value)` | Int | Convert from Float (truncates), Bool (`true`=1), or String |
| `to_float` | `(value)` | Float | Convert from Int or String |

### Null handling (1)

| Function | Signature | Returns | Description |
|---|---|---|---|
| `coalesce` | `(value, value, ...)` | first non-null | Returns the first non-null argument. Variadic (at least 1 arg) |

## Examples

Guards:

```yaml
guard: "event.plan != entity.plan"
guard: "event.amount > 0 and days_since(entity.last_purchase) < 365"
guard: "contains(event.email, '@company.com')"
guard: "len(event.items) > 0"
```

Computed properties:

```yaml
compute: "entity.mrr * 12"
compute: "if days_since(entity.last_order) > 90 then 'inactive' else 'active'"
compute: "date_trunc(entity.signup_date, 'month')"
```

Field mappings:

```yaml
mappings:
  domain: { compute: "split(event.email, '@')[1]" }
  full_name: { compute: "str::trim(event.first_name + ' ' + event.last_name)" }
```

Identity normalization:

```yaml
identity:
  email:
    normalize: "str::to_lowercase(str::trim(value))"
```

Duration arithmetic:

```yaml
guard: "event.timestamp > entity.signup_date + 30d"
```
