import shutil
import unittest
from unittest.mock import patch, mock_open, MagicMock
import tempfile
import os
import json
import subprocess
import pytest


@pytest.fixture
def temp_dir():
    dir_path = os.getcwd() + '/temp_dir'
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.mkdir(dir_path)
    yield dir_path

def test_command_example_01(temp_dir):
    # Test with a negative file count
    file_count = -1
    file_name = 'test'
    data_lines = 10
    prefix = 'count'
    data_schema = '{"name": "name", "age": "age"}'
    command = [
        'python3', 'datagen.py',
        str(temp_dir),
        '--path_to_save_files', temp_dir,
        '--file_name', file_name,
        '--data_lines', str(data_lines),
        '--files_count', str(file_count),
        '--file_prefix', prefix,
        '--data_schema', data_schema
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode == 1
    assert "Error: files_count cannot be less than 0" in result.stderr


def test_command_example_02(temp_dir):
    # Test with a schema string
    file_count = 1
    file_name = 'test'
    data_lines = 10
    prefix = 'count'
    data_schema = '{"name": "str:rand", "age": "int:rand"}'
    command = [
        'python3', 'datagen.py',
        str(temp_dir),
        '--path_to_save_files', temp_dir,
        '--file_name', file_name,
        '--data_lines', str(data_lines),
        '--files_count', str(file_count),
        '--file_prefix', prefix,
        '--data_schema', data_schema
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode == 0
    assert os.path.exists(f'{temp_dir}/{file_name}.json')
    with open(f'{temp_dir}/{file_name}.json', 'r') as f:
        data = json.load(f)
        assert len(data) == data_lines
        for record in data:
            assert 'name' in record
            assert 'age' in record
            assert isinstance(record['name'], str)
            assert isinstance(record['age'], int)


def test_command_example_03(temp_dir):
    # Test with a schema file
    file_count = 1
    file_name = 'test'
    data_lines = 10
    prefix = 'count'
    data_schema = '{"name": "str:rand", "age": "int:rand"}'
    schema_file = f'{temp_dir}/schema.json'
    with open(schema_file, 'w') as f:
        f.write(data_schema)
    command = [
        'python3', 'datagen.py',
        str(temp_dir),
        '--path_to_save_files', temp_dir,
        '--file_name', file_name,
        '--data_lines', str(data_lines),
        '--files_count', str(file_count),
        '--file_prefix', prefix,
        '--data_schema', schema_file
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode == 0
    assert os.path.exists(f'{temp_dir}/{file_name}.json')
    with open(f'{temp_dir}/{file_name}.json', 'r') as f:
        data = json.load(f)
        assert len(data) == data_lines
        for record in data:
            assert 'name' in record
            assert 'age' in record
            assert isinstance(record['name'], str)
            assert isinstance(record['age'], int)


def test_command_example_04(temp_dir):
    # Test with an empty array in schema
    file_count = 1
    file_name = 'test'
    data_lines = 10
    prefix = 'count'
    data_schema = {"name": "str:[]", "age": "int:rand"}
    schema_file = f'{temp_dir}/schema.json'
    with open(schema_file, 'w') as f:
        json.dump(data_schema, f)
    command = [
        'python3', 'datagen.py',
        str(temp_dir),
        '--path_to_save_files', temp_dir,
        '--file_name', file_name,
        '--data_lines', str(data_lines),
        '--files_count', str(file_count),
        '--file_prefix', prefix,
        '--data_schema', schema_file
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode == 1
    assert f"ERROR - Error: name has no elements" in result.stderr