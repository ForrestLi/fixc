from typing import List, FrozenSet, Union
from itertools import combinations
from collections import OrderedDict
import collections


class RepeatedTagError(ValueError):

    def __init__(self, message='', *, offending_tag):
        detail = '(offending tag: {})'.format(offending_tag)
        if not message:
            message = detail
        else:
            message += (' ' + detail)
        super().__init__(message)
        self.offending_tag = offending_tag


class Group(collections.abc.MutableMapping):

    def __init__(self, iter_init: Union[OrderedDict, List]=None, *,
                 req_tags: FrozenSet[int] = frozenset(),
                 is_top_level=False, req_cond=None):
        """
           dictionary representing tag value pairs; value can be list of groups;
           value cannot be empty list; None value means no such tag value pair;
           the id tag is the first tag of self

        req_tags: set of required tag numbers
        req_cond: list of 2-tuple
            (<lambda as a boolean condition to check; take an argument
            which self will be passed in>, error message);
            used to do extra semantic check
        """
        self._d = OrderedDict()
        if iter_init is not None:
            if isinstance(iter_init, list):
                self.update((x, None) for x in iter_init)
            elif isinstance(iter_init, dict):
                self._d.update(iter_init)
            else:
                raise TypeError('iter init is not dict or list')

        self.req_tags = req_tags
        self.req_cond = [] if req_cond is None else req_cond
        self.is_top_level = is_top_level
        self.error_msg = ''

    def __setitem__(self, key, item):
        if isinstance(key, tuple):
            x = self._d
            for i in key[:-1]:
                x = x[i]
            x[key[-1]] = item
        else:
            self._d[key] = item

    def __getitem__(self, key):
        if isinstance(key, tuple):
            x = self._d
            for i in key:
                x = x[i]
            return x
        else:
            return self._d[key]

    def __delitem__(self, key):
        if isinstance(key, tuple):
            x = self._d
            for i in key[:-1]:
                x = x[i]
            del x[key[-1]]
        else:
            del self._d[key]

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)

    def move_to_end(self, *args, **kwargs):
        self._d.move_to_end(*args, **kwargs)

    def merge(self, group):  # group: Group
        self.update(group.items())
        self.req_tags |= group.req_tags
        self.req_cond += group.req_cond

    def add_inner_group(self, group):
        if self.get(group.id_tag, None) is None:
            self[group.id_tag] = [group]
        elif isinstance(self[group.id_tag], list):
            self[group.id_tag].append(group)
        else:
            raise ValueError(
                'group id tag\'s value not a list (of groups) or None')
        self.move_to_end(group.id_tag)

    @property
    def id_tag(self):
        return next(iter(self))

    def is_valid_semantics(self):
        self.error_msg = ''
        is_empty = not self
        invalid_value_type_tags = [t for t, v in self.items() if not (
            isinstance(v, bytes) or (isinstance(v, list) and all(
                isinstance(g, Group) for g in v)))]
        invalid_group_tags = [t for t, v in self.items() if isinstance(
            v, list) and not all(
                g.id_tag == t for g in v)]
        non_int_tags = [k for k in self if not isinstance(k, int)]
        missing_required_tags = tuple(
            t for t in set(self.req_tags) if t not in self)
        recur_invalid_groups = []
        if not invalid_value_type_tags:
            recur_invalid_groups += [g for t, v in self.items()
                                     if isinstance(v, list) for g in v
                                     if not g.is_valid_semantics()]
        all_req_cond_pass = all(f(self) for f, _ in self.req_cond)
        check_bool = not any((
            invalid_value_type_tags, non_int_tags, invalid_group_tags,
            recur_invalid_groups, missing_required_tags)) and not is_empty \
            and all_req_cond_pass

        if not check_bool:
            errors = []
            if is_empty:
                errors.append('{}: empty group'.format(self))
            if invalid_value_type_tags:
                errors.append('{}: invalid type for value of tags {}'.format(
                    self, invalid_value_type_tags))
            if non_int_tags:
                errors.append('{}: non int tags {}'.format(
                    self, non_int_tags))
            if invalid_group_tags:
                errors.append(
                    '{}: tag {} has group without the same id tag'.format(
                        self, invalid_group_tags))
            if missing_required_tags:
                errors.append('{}: missing required tags {}'.format(
                    self, missing_required_tags))
            if recur_invalid_groups:
                errors.extend([g.error_msg for g in recur_invalid_groups])
            if not all_req_cond_pass:
                errors += [y for f, y in self.req_cond if not f(self)]

            self.error_msg = ', '.join(errors)
        return check_bool

    def __repr__(self):
        return 'GRP' + str(list(self._d.items()))

    def build(self, *, delim=b'\x01'):
        if not self.is_valid_semantics():
            raise ValueError(self.error_msg)
        return b''.join([(b''.join([x.build(delim=delim) for x in v])
                          if isinstance(v, list)
                          else (str(k).encode() + b'=' + v) + delim)
                         for k, v in self.items()])

    def iter_tag_value(self):
        for t, v in self.items():
            if isinstance(v, list):
                for group in v:
                    for tg, vg in group.iter_tag_value():
                        yield tg, vg
            else:
                yield t, v


class GroupStructure(collections.abc.MutableMapping):

    def __init__(self, iter_init: Union[OrderedDict, List]=None,
                 req_tags: FrozenSet[int] = frozenset(), is_top_level=False,
                 validate_construct=True):
        self._d = OrderedDict()
        if iter_init is not None:
            if isinstance(iter_init, list):
                self.update((x, None) for x in iter_init)
            elif isinstance(iter_init, dict):
                self._d.update(iter_init)
            else:
                raise TypeError('iter init is not dict or list')

        self.req_tags = req_tags
        self.error_msg = ''
        self.is_top_level = is_top_level
        if validate_construct and not self.is_valid_construct():
            raise ValueError(self.error_msg)

    def __setitem__(self, key, item):
        if isinstance(key, tuple):
            x = self._d
            for i in key[:-1]:
                x = x[i]
            x[key[-1]] = item
        else:
            self._d[key] = item

    def __getitem__(self, key):
        if isinstance(key, tuple):
            x = self._d
            for i in key:
                x = x[i]
            return x
        else:
            return self._d[key]

    def __delitem__(self, key):
        if isinstance(key, tuple):
            x = self._d
            for i in key[:-1]:
                x = x[i]
            del x[key[-1]]
        else:
            del self._d[key]

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)

    def move_to_end(self, *args, **kwargs):
        self._d.move_to_end(*args, **kwargs)

    @property
    def inner_groups(self):
        return [v for v in self.values() if isinstance(v, GroupStructure)]

    def __repr__(self):
        return 'GRPSTRUCT' + str(list(self._d.items()))

    def is_valid_construct(self, recursive=True):
        self.error_msg = ''
        if not self:
            raise ValueError('not initialized or Empty Group')
        non_none_or_groupstructure_tags = [
            t for t, v in self.items()
            if not(isinstance(v, GroupStructure) or v is None)]
        missing_required_tags = tuple(
            t for t in set(self.req_tags) if t not in self)
        recur_invalid_gs = [v for t, v in self.items() if isinstance(
            v, GroupStructure) and not v.is_valid_construct()]
        is_intersecting_group = is_intersecting_groups(self.inner_groups)
        check_bool = not non_none_or_groupstructure_tags and \
            not missing_required_tags and not recur_invalid_gs and \
            not is_intersecting_group
        if not check_bool:
            err_msgs = []
            if missing_required_tags:
                err_msgs.append(
                    'In {}: missing required tags: {}'.format(
                        self, missing_required_tags))
            if non_none_or_groupstructure_tags:
                err_msgs.append(
                    'In {}: value of tags {} is not None or '
                    'of type GroupStructure'.format(
                        self, non_none_or_groupstructure_tags))
            if is_intersecting_group:
                err_msgs.append('{}: has intersecting groups'.format(self))
            if recur_invalid_gs:
                err_msgs.append(
                    ','.join([g.error_msg for g in recur_invalid_gs]))
            self.error_msg = ', '.join(err_msgs)
        return check_bool

    @property
    def id_tag(self):
        return next(iter(self))

    def current_level_to_group(self):
        return Group(
            iter_init=OrderedDict(
                [] if self.is_top_level else [(self.id_tag, None)]),
            req_tags=self.req_tags,
            is_top_level=self.is_top_level)


def is_intersecting_groups(groups: List[GroupStructure]):
    return any(set(g1) & set(g2) for g1, g2 in combinations(groups, 2))
