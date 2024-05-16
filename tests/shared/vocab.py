import pytest


def vocab():
    return {
        "columns": {
            "column1": {
                "term": "column_one"
            },
            "column2": {
                "term": "column_two",
                "sub1": {
                    "term": "sub_one"
                },
                "sub2": {
                    "term": "sub_two"
                }
            },
            "column3": {
                "term": "column_three",
            },
            "column4": {
                "term": "column_four",
            },
            "column5": {
                "term": "column_five",
            },
            "column6": {
                "term": "column_six",
                "sub1": {
                    "term": "sub_one"
                },
                "sub2": {
                    "term": "sub_two"
                },
                "sub3": {
                    "term": "sub_three"
                }
            },
            "column7": {
                "term": "column_seven",
                "sub1": {
                    "term": "sub_one"
                },
                "sub2": {
                    "term": "sub_two",
                    "sub2-1": {
                        "term": "sub_two_one",
                    },
                    "sub2-2": {
                        "term": "sub_two_two",
                    }
                },
                "sub3": {
                    "term": "sub_three",
                    "sub3-1": {
                        "term": "sub_three_one",
                    },
                    "sub3-2": {
                        "term": "sub_three_two",
                    }
                }
            }
        }
    }
