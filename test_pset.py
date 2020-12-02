#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `final_project` package."""

from unittest import TestCase
from final_project.generate_coverage import return_foo


class FakeFileFailure(IOError):
    pass


class BasicTests(TestCase):
    def test_foo(self):
        self.assertEqual(return_foo(), "Foo")
