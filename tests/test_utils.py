# -*- coding: utf-8 -*-
import itertools

from cabbage.utils import ExponentialBackoff, FibonaccianBackoff


class TestExponentialBackoff:
    def test_double(self):
        backoff = ExponentialBackoff(1, 2, 2048)
        assert list(itertools.islice(backoff, 15)) == [
            1,
            2,
            4,
            8,
            16,
            32,
            64,
            128,
            256,
            512,
            1024,
            2048,
            2048,
            2048,
            2048,
        ]


class TestFibonaccianBackoff:
    def test_maxprecision(self):
        backoff = FibonaccianBackoff(1000)
        assert list(itertools.islice(backoff, 15)) == [
            1,
            1,
            2,
            3,
            5,
            8,
            13,
            21,
            34,
            55,
            89,
            144,
            233,
            377,
            610,
        ]

    def test_1min(self):
        backoff = FibonaccianBackoff(60)
        assert list(itertools.islice(backoff, 15)) == [
            1,
            1,
            2,
            3,
            5,
            8,
            13,
            21,
            34,
            55,
            60,
            60,
            60,
            60,
            60,
        ]
