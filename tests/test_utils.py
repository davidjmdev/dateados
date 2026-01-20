"""Tests para las utilidades del modulo de ingesta.

Estos tests verifican las funciones de conversion, normalizacion y
parsing que se usan durante el proceso de ingesta.
"""

import pytest
from datetime import date, datetime, timedelta
from ingestion.utils import (
    convert_minutes_to_interval,
    normalize_season,
    parse_game_id,
    parse_date,
    safe_int,
    safe_int_or_none,
    safe_float,
)


class TestConvertMinutesToInterval:
    """Tests para convert_minutes_to_interval()."""
    
    # =========================================================================
    # Formato MM:SS (estandar)
    # =========================================================================
    
    def test_standard_format(self):
        """Formato estandar MM:SS."""
        assert convert_minutes_to_interval("35:30") == timedelta(minutes=35, seconds=30)
    
    def test_single_digit_minutes(self):
        """Minutos de un solo digito."""
        assert convert_minutes_to_interval("5:30") == timedelta(minutes=5, seconds=30)
    
    def test_zero_seconds(self):
        """Cero segundos."""
        assert convert_minutes_to_interval("35:00") == timedelta(minutes=35)
    
    def test_large_minutes(self):
        """Muchos minutos (overtime)."""
        assert convert_minutes_to_interval("65:00") == timedelta(minutes=65)
    
    def test_max_seconds(self):
        """Segundos al maximo (59)."""
        assert convert_minutes_to_interval("10:59") == timedelta(minutes=10, seconds=59)
    
    # =========================================================================
    # Formato HH:MM:SS
    # =========================================================================
    
    def test_hours_format(self):
        """Formato con horas HH:MM:SS."""
        result = convert_minutes_to_interval("1:05:30")
        expected = timedelta(hours=1, minutes=5, seconds=30)
        assert result == expected
    
    def test_hours_format_large(self):
        """Muchas horas (partidos con multiples overtimes)."""
        result = convert_minutes_to_interval("2:30:00")
        expected = timedelta(hours=2, minutes=30)
        assert result == expected
    
    # =========================================================================
    # Formato decimal
    # =========================================================================
    
    def test_decimal_format(self):
        """Formato decimal (minutos como float)."""
        result = convert_minutes_to_interval("35.5")
        expected = timedelta(minutes=35, seconds=30)
        assert result == expected
    
    def test_decimal_whole_number(self):
        """Decimal sin fraccion."""
        result = convert_minutes_to_interval("30.0")
        expected = timedelta(minutes=30)
        assert result == expected
    
    def test_decimal_quarter(self):
        """Decimal con .25 (15 segundos)."""
        result = convert_minutes_to_interval("10.25")
        expected = timedelta(minutes=10, seconds=15)
        assert result == expected
    
    # =========================================================================
    # Casos edge
    # =========================================================================
    
    def test_empty_string(self):
        """String vacio retorna zero."""
        assert convert_minutes_to_interval("") == timedelta(0)
    
    def test_none_value(self):
        """None retorna zero."""
        assert convert_minutes_to_interval(None) == timedelta(0)
    
    def test_zero_minutes(self):
        """Cero minutos."""
        assert convert_minutes_to_interval("0:00") == timedelta(0)
    
    def test_just_zero(self):
        """Solo cero como string."""
        assert convert_minutes_to_interval("0") == timedelta(0)


class TestNormalizeSeason:
    """Tests para normalize_season()."""
    
    def test_already_normalized(self):
        """Ya esta en formato correcto."""
        assert normalize_season("2023-24") == "2023-24"
    
    def test_no_dash(self):
        """Sin guion (6 digitos)."""
        assert normalize_season("202324") == "2023-24"
    
    def test_year_only(self):
        """Solo el ano inicial (4 digitos)."""
        assert normalize_season("2023") == "2023-24"
    
    def test_with_spaces(self):
        """Con espacios alrededor del guion."""
        assert normalize_season("2023 - 24") == "2023-24"
    
    def test_century_boundary(self):
        """Cambio de siglo."""
        assert normalize_season("1999") == "1999-00"
    
    def test_old_season(self):
        """Temporada antigua."""
        assert normalize_season("1983-84") == "1983-84"
    
    def test_future_season(self):
        """Temporada futura."""
        assert normalize_season("2030") == "2030-31"


class TestParseGameId:
    """Tests para parse_game_id()."""
    
    # =========================================================================
    # Tipos de partidos
    # =========================================================================
    
    def test_regular_season(self):
        """Game ID de temporada regular."""
        result = parse_game_id("0022300123")
        assert result['type'] == 'RS'
    
    def test_playoffs(self):
        """Game ID de playoffs."""
        result = parse_game_id("0042300123")
        assert result['type'] == 'PO'
    
    def test_playin(self):
        """Game ID de PlayIn."""
        result = parse_game_id("0052300001")
        assert result['type'] == 'PI'
    
    def test_ist_final(self):
        """Game ID de final del In-Season Tournament."""
        result = parse_game_id("0062300001")
        assert result['type'] == 'IST'
    
    def test_preseason(self):
        """Game ID de pretemporada."""
        result = parse_game_id("0012300001")
        assert result['type'] == 'PRESEASON'
    
    def test_allstar(self):
        """Game ID de All-Star Game."""
        result = parse_game_id("0032300001")
        assert result['type'] == 'ALLSTAR'
    
    def test_unknown_type(self):
        """Game ID con prefijo desconocido."""
        result = parse_game_id("0092300001")
        assert result['type'] == 'unknown'
    
    # =========================================================================
    # Extraccion de fecha (formato largo)
    # =========================================================================
    
    def test_long_format_with_date(self):
        """Game ID largo con fecha embebida."""
        # Formato: 00X YYYY MM DD NNN (14 chars)
        result = parse_game_id("00220231015001")
        assert result['type'] == 'RS'
        assert result['date'] == date(2023, 10, 15)
        assert result['season'] == '2023-24'
    
    def test_long_format_spring_game(self):
        """Partido en primavera (pertenece a temporada anterior)."""
        result = parse_game_id("00220240315001")
        assert result['date'] == date(2024, 3, 15)
        assert result['season'] == '2023-24'
    
    # =========================================================================
    # Casos edge
    # =========================================================================
    
    def test_short_id(self):
        """ID corto (10 chars) no tiene fecha."""
        result = parse_game_id("0022300123")
        assert result['date'] is None
        assert result['season'] is None
    
    def test_empty_id(self):
        """ID vacio."""
        result = parse_game_id("")
        assert result['type'] == 'unknown'
        assert result['date'] is None
    
    def test_none_id(self):
        """ID None."""
        result = parse_game_id(None)
        assert result['type'] == 'unknown'
    
    def test_too_short(self):
        """ID demasiado corto."""
        result = parse_game_id("00")
        assert result['type'] == 'unknown'


class TestParseDate:
    """Tests para parse_date()."""
    
    def test_iso_format(self):
        """Formato ISO YYYY-MM-DD."""
        assert parse_date("2024-01-15") == date(2024, 1, 15)
    
    def test_datetime_object(self):
        """Objeto datetime - retorna el mismo objeto porque datetime es subclase de date."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        # Nota: datetime es subclase de date, por lo que isinstance(dt, date) es True
        # El codigo retorna el objeto tal cual, lo cual es aceptable para el uso previsto
        result = parse_date(dt)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
    
    def test_date_object(self):
        """Objeto date (passthrough)."""
        d = date(2024, 1, 15)
        assert parse_date(d) == d
    
    def test_us_format(self):
        """Formato US MM/DD/YYYY."""
        assert parse_date("01/15/2024") == date(2024, 1, 15)
    
    def test_with_timezone(self):
        """Formato ISO con timezone."""
        assert parse_date("2024-01-15T10:30:00Z") == date(2024, 1, 15)
    
    def test_with_timezone_offset(self):
        """Formato ISO con offset de timezone."""
        assert parse_date("2024-01-15T10:30:00-05:00") == date(2024, 1, 15)
    
    def test_none_value(self):
        """None retorna None."""
        assert parse_date(None) is None
    
    def test_invalid_string(self):
        """String invalido retorna None."""
        assert parse_date("not-a-date") is None
    
    def test_empty_string(self):
        """String vacio retorna None."""
        assert parse_date("") is None


class TestSafeInt:
    """Tests para safe_int()."""
    
    def test_valid_int_string(self):
        """String con entero valido."""
        assert safe_int("10") == 10
    
    def test_float_string(self):
        """String con float se trunca."""
        assert safe_int("10.7") == 10
    
    def test_negative(self):
        """Numero negativo."""
        assert safe_int("-5") == -5
    
    def test_zero(self):
        """Cero."""
        assert safe_int("0") == 0
    
    def test_none_returns_default(self):
        """None retorna el default (0)."""
        assert safe_int(None) == 0
    
    def test_none_with_custom_default(self):
        """None retorna custom default."""
        assert safe_int(None, default=-1) == -1
    
    def test_invalid_string(self):
        """String invalido retorna default."""
        assert safe_int("abc") == 0
    
    def test_empty_string(self):
        """String vacio retorna default."""
        assert safe_int("") == 0
    
    def test_whitespace(self):
        """Solo espacios retorna default."""
        assert safe_int("   ") == 0
    
    def test_scientific_notation(self):
        """Notacion cientifica."""
        assert safe_int("1e2") == 100
    
    def test_int_input(self):
        """Input ya es int."""
        assert safe_int(42) == 42
    
    def test_float_input(self):
        """Input es float."""
        assert safe_int(42.9) == 42


class TestSafeIntOrNone:
    """Tests para safe_int_or_none()."""
    
    def test_valid_positive(self):
        """Entero positivo valido."""
        assert safe_int_or_none("10") == 10
    
    def test_zero_returns_none(self):
        """Cero retorna None (util para campos donde 0 no es valido)."""
        assert safe_int_or_none("0") is None
    
    def test_none_returns_none(self):
        """None retorna None."""
        assert safe_int_or_none(None) is None
    
    def test_invalid_returns_none(self):
        """String invalido retorna None."""
        assert safe_int_or_none("abc") is None
    
    def test_negative_returns_none(self):
        """Negativo retorna None (no hay valores negativos validos)."""
        assert safe_int_or_none("-5") is None
    
    def test_float_truncates(self):
        """Float se trunca."""
        assert safe_int_or_none("10.7") == 10
    
    def test_float_less_than_one(self):
        """Float menor a 1 retorna None."""
        assert safe_int_or_none("0.5") is None


class TestSafeFloat:
    """Tests para safe_float()."""
    
    def test_valid_float(self):
        """Float valido."""
        assert safe_float("10.5") == 10.5
    
    def test_integer_string(self):
        """Entero como string."""
        assert safe_float("10") == 10.0
    
    def test_negative(self):
        """Numero negativo."""
        assert safe_float("-5.5") == -5.5
    
    def test_zero(self):
        """Cero."""
        assert safe_float("0") == 0.0
    
    def test_none_returns_default(self):
        """None retorna el default (0.0)."""
        assert safe_float(None) == 0.0
    
    def test_none_with_custom_default(self):
        """None retorna custom default."""
        assert safe_float(None, default=-1.0) == -1.0
    
    def test_invalid_string(self):
        """String invalido retorna default."""
        assert safe_float("abc") == 0.0
    
    def test_scientific_notation(self):
        """Notacion cientifica."""
        assert safe_float("1.5e2") == 150.0
    
    def test_percentage_format(self):
        """Porcentaje como decimal."""
        assert safe_float("0.545") == 0.545
    
    def test_float_input(self):
        """Input ya es float."""
        assert safe_float(42.5) == 42.5
