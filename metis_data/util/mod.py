import importlib


def module_entry_point(mod_name, entry_point):
    return getattr(import_module(mod_name), entry_point)


def import_module(mod_name: str):
    return importlib.import_module(mod_name)
