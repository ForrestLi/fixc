from fix.group import (
    Group,
    is_intersecting_groups,
    RepeatedTagError,
    GroupStructure)
from fix.util import iter_rawmsg, fix_time_now
import collections
from collections import OrderedDict
import itertools


class Message(collections.abc.MutableMapping):

    class State:
        def __init__(
                self,
                group_stack,
                output_group_stack,
                curr_group,
                output_group):
            self.group_stack = group_stack
            self.output_group_stack = output_group_stack
            self.curr_group = curr_group
            self.output_group = output_group

    """
    store as an OrderedDict mapping tag to value
    to access groups: fix = Fix(...);
        fix[555][0] to get the first group of tag 555
        fix[555][0][555] to get the tag value of tag 555 of the first group
    """
    @classmethod
    def parse(cls, raw_msg, init_group: GroupStructure=None, *,
              delim=b'\x01', validate_construct=True, validate_semantics=True,
              auto_reset=False):
        if init_group is not None and validate_construct:
            if is_intersecting_groups(init_group.inner_groups):
                raise ValueError('intersecting groups exist')
            if not init_group.is_valid_construct():
                raise ValueError(init_group.error_msg)
            if not isinstance(init_group, GroupStructure):
                raise TypeError('init_group not of type GroupStructure')

        if init_group is None:
            init_group = GroupStructure(
                validate_construct=False, is_top_level=True)
        init_group.is_top_level = True

        state = Message.State([], [], init_group,
                              init_group.current_level_to_group())

        def attach_group(curr: Group, outer: Group,
                         validate_semantics=validate_semantics):
            if validate_semantics and not curr.is_valid_semantics():
                raise ValueError('encountered invalid group: {}, {}'.format(
                    curr, curr.error_msg))
            outer.add_inner_group(curr)

        def try_append_next_level_group(tag, value, state):
            next_level_group = next(
                (g for g in state.curr_group.inner_groups if tag == g.id_tag),
                None)
            if next_level_group is not None:
                # the curr_group becomes the outer group
                state.group_stack.append(state.curr_group)
                state.output_group_stack.append(state.output_group)

                # new group
                state.output_group = next_level_group.current_level_to_group()

                state.curr_group = next_level_group
                state.output_group[tag] = value
                return True
            return False

        def try_append_same_level_group(tag, value, state):
            same_level_group = next(
                (g for g in state.group_stack[-1].inner_groups
                 if tag == g.id_tag),
                None) if not state.curr_group.is_top_level else None
            if same_level_group is not None:
                # init_group can't have same level group
                attach_group(state.output_group, state.output_group_stack[-1])
                state.curr_group = same_level_group

                # new group
                state.output_group = same_level_group.current_level_to_group()
                state.output_group[tag] = value
                return True
            return False

        for t, v in iter_rawmsg(raw_msg, delim=delim):
            # case 1:
            #        new next level group
            #        push the current group into the stack, current group is now
            #        the new group
            # case 2:
            #        new same level group; append current group to current level
            #        group; current group is now a new group
            # case 3:
            #        not match same/next level group tag id: pop group from
            #        stack
            try:
                t = int(t)
            except ValueError as e:
                raise ValueError('tag not integer: "{}"'.format(t)) from e
            if try_append_next_level_group(t, v, state):
                continue
            if try_append_same_level_group(t, v, state):
                continue
            while t not in state.curr_group and \
                    not state.curr_group.is_top_level:
                attach_group(state.output_group, state.output_group_stack[-1])
                state.curr_group = state.group_stack.pop()
                state.output_group = state.output_group_stack.pop()
            if try_append_next_level_group(t, v, state):
                continue
            if try_append_same_level_group(t, v, state):
                continue
            if t in state.output_group:
                raise RepeatedTagError(offending_tag=t)
            state.output_group[t] = v
        # pop all the groups (in case fixmsg doesn't end with tag 10 and end
        # with an inner group last tag)
        while state.output_group_stack:
            attach_group(state.output_group, state.output_group_stack[-1])
            state.curr_group = state.group_stack.pop()
            state.output_group = state.output_group_stack.pop()
        return cls(state.output_group, delim=delim,
                   auto_reset=auto_reset, init_groupstructure=init_group)

    def __init__(self, initialized_group: Group=None, *, delim: bytes=b'\x01',
                 validate_semantics=True,
                 auto_reset=False, reset_id_time=True, reset_ht=True,
                 init_groupstructure=None):
        """
           dictionary representing tag value pairs; value can be list of groups;
           value cannot be empty list
        reset_ht: reset header trailer
        """
        self.error_msg = ''
        self._initialized_group = initialized_group
        self.init_groupstructure = init_groupstructure
        self.delim = delim
        self.auto_reset = auto_reset
        if validate_semantics and not self.is_valid_semantics():
            raise ValueError(', '.join(
                (self._initialized_group.error_msg, self.error_msg))
            )
        if reset_id_time:
            self.reset(seqnum=int(self.seqnum.decode()))
        elif reset_ht:
            self.reset_bodylen_checksum(seqnum=int(self.seqnum.decode()))
        if validate_semantics and not self.is_valid_header_trailer():
            raise ValueError('invalid bodylen/checksum')

    def __setitem__(self, key, item):
        self._initialized_group[key] = item
        if self.auto_reset:
            self.reset(seqnum=self.seqnum)

    def __getitem__(self, key):
        x = self._initialized_group[key]
        return x

    def __delitem__(self, key):
        del self._initialized_group[key]
        if self.auto_reset:
            self.reset(seqnum=self.seqnum)

    def __iter__(self):
        return iter(self._initialized_group)

    def __len__(self):
        return len(self._initialized_group)

    def is_valid_checksum(self):
        checksum = sum(sum(str(x).encode()) + sum(y) + 1 + ord(b'=')
                       for x, y in self.iter_tag_value() if x != 10) % 256
        return checksum == int(self.checksum.decode())

    def is_valid_bodylen(self):
        bodylen = sum(len(str(x).encode()) + len(y) + 2
                      for x, y in self.iter_tag_value()
                      if x not in (8, 9, 10))
        return bodylen == int(self.bodylen.decode())

    def is_valid_cond(self):
        return True

    def is_valid_header_trailer(self):
        return self.is_valid_bodylen() and self.is_valid_checksum()

    def is_valid_semantics(self):
        # TODO: check non-consecutive groups of same id tag
        # This is guaranteed to be true if the message is initialized by parse
        return self._initialized_group.is_valid_semantics()

    def reset_bodylen_checksum(self, *, seqnum, extra=None):
        self.reset(clordid=False, transacttime=False, sendingtime=False,
                   extra=extra, seqnum=seqnum)

    def reset(self, *, seqnum, initialized_group: Group=None, clordid=True,
              transacttime=True, sendingtime=True, bodylen=True, checksum=True,
              extra=None):
        orig_auto_reset = self.auto_reset
        self.auto_reset = False
        try:
            if initialized_group is not None:
                self._initialized_group = initialized_group

            tlg = self._initialized_group
            if sendingtime:
                tlg[52] = fix_time_now()
            if transacttime:
                tlg[60] = fix_time_now()
            if clordid:
                tlg[11] = str(Message.message_counter).encode() + \
                    b'-' + fix_time_now(us=True, fmt='%H%M%S.%f')
                Message.message_counter += 1
            if seqnum is not None:
                tlg[34] = str(seqnum).encode()
            if extra:
                tlg.update(extra)

            # body_len and checksum need to account for b'=' and b'\x01'
            if bodylen:
                body_len = sum(len(str(x).encode()) + len(y) + 2
                               for x, y in tlg.iter_tag_value()
                               if x not in (8, 9, 10))
                tlg[9] = str(body_len).encode()

            for k in list(self.keys()):
                if k not in MessageWithHeader.HEADER_TAGS:
                    self._initialized_group.move_to_end(k)
            if checksum:
                checksum = sum(sum(str(x).encode()) + sum(y) + 1 + ord(b'=')
                               for x, y in tlg.iter_tag_value()
                               if x != 10) % 256
                tlg[10] = str(checksum).zfill(3).encode()
                tlg.move_to_end(10)
                if 35 in tlg:
                    tlg.move_to_end(35, last=False)
                tlg.move_to_end(9, last=False)
                tlg.move_to_end(8, last=False)
        finally:
            self.auto_reset = orig_auto_reset

    def iter_tag_value(self):
        for t, v in self._initialized_group.iter_tag_value():
            yield t, v

    def iter_inner_group_id_tags(self):
        for t, v in self.items():
            if isinstance(v, list):
                yield t

    @property
    def clordid(self):
        return self[11]

    @property
    def msgtype(self):
        return self[35]

    @property
    def seqnum(self):
        return self[34]

    @property
    def checksum(self):
        return self[10]

    @property
    def bodylen(self):
        return self[9]

    @property
    def qty(self):
        return self[38]

    @qty.setter
    def qty(self, value):
        if isinstance(value, int):
            value = str(value).encode()
        self[38] = value

    @property
    def px(self):
        return self[44]

    @px.setter
    def px(self, value):
        if isinstance(value, int):
            value = str(value).encode()
        self[44] = value

    @property
    def ordtype(self):
        return self[40]

    @ordtype.setter
    def ordtype(self, value):
        self[40] = value

    @property
    def side(self):
        return self[54]

    @side.setter
    def side(self, value):
        self[54] = value

    @property
    def symbol(self):
        return self[55]

    @symbol.setter
    def symbol(self, value):
        self[55] = value

    def __repr__(self):
        return repr(self._initialized_group)

    def __bytes__(self):
        return self._initialized_group.build(delim=self.delim)

    message_counter = 1


class MessageWithHeader(Message):

    HEADER_TAGS = frozenset(
        (8, 9, 35, 1128, 1129, 49, 56, 115, 1282, 90, 91, 34, 50,
         142, 57, 143, 116, 144, 1292, 1452, 43, 97, 52, 122, 212,
         213, 347, 369, 98, 108, 95, 96, 141, 789, 383, 464, 553,
         554, 1137))
    HEADER_REQ_TAGS = frozenset((8, 9, 35, 49, 56, 34))

    def __init__(self, initialized_group: Group=None, *,
                 default_header=None, reset_ht=True, reset_id_time=True,
                 **kwargs):
        if default_header is None:
            default_header = OrderedDict([(8, b'FIX.4.2'), (9, b''),
                                          (98, b'0'), (108, b'20')])
        g = Group(default_header, is_top_level=True,
                  req_tags=self.HEADER_REQ_TAGS)

        if reset_id_time:
            g[52] = b''
            g[60] = b''
            g[11] = b''
        if initialized_group is not None:
            for k in list(initialized_group.keys()):
                if k not in self.HEADER_TAGS:
                    initialized_group.move_to_end(k)
            g.merge(initialized_group)

        super().__init__(initialized_group=g, reset_id_time=reset_id_time,
                         reset_ht=reset_ht, **kwargs)

    def is_valid_cond(self):
        keys = self.keys()
        if not all(x in keys for x in (8, 9, 35, 10)):
            self.error_msg = 'missing tag 8, 9, 35, or 10'
            return False
        kl = list(keys)
        valid_init_tail_tags = \
            kl[0] == 8 and kl[1] == 9 and kl[2] == 35 and kl[-1] == 10
        g = itertools.groupby(
            self.keys(), key=lambda x: x in MessageWithHeader.HEADER_TAGS)
        header_group = list(next(g)[1])
        others = list(next(g)[1])
        # all header tags must be on the front
        complete_partition = \
            len(header_group) + len(others) == len(self) and \
            all(x not in MessageWithHeader.HEADER_TAGS for x in others)
        valid_ht = self.is_valid_header_trailer()
        check_bool = valid_init_tail_tags and complete_partition and \
            valid_ht and super().is_valid_cond()
        if not check_bool:
            errors = []
            if not valid_init_tail_tags:
                errors.append('init tags not 8, 9, 35 or last tag not 10')
            if not complete_partition:
                errors.append('not all header tags on the front:'
                              ' header_group={}(len={}),'
                              ' others={}(len={}), others in ht={}'
                              ', self.keys()={}(len={})'.format(
                                  header_group, len(header_group),
                                  others, len(others),
                                  list(x in MessageWithHeader.HEADER_TAGS
                                       for x in others),
                                  list(self.keys()),
                                  len(self)))
            if not valid_ht:
                errors.append('invalid bodylen/checksum')
            self.error_msg = ', '.join(errors)

        return check_bool

    def __bytes__(self):
        if not self.is_valid_cond():
            raise ValueError(self.error_msg)
        return self._initialized_group.build(delim=self.delim)


class LogonMessage(MessageWithHeader):

    def __init__(self, initialized_group: Group=None, *args, **kwargs):
        g = Group(OrderedDict([(35, b'A'), (141, b'Y')]))
        if initialized_group is not None:
            g.merge(initialized_group)
        super().__init__(g, *args, **kwargs)


class LogoutMessage(MessageWithHeader):

    def __init__(self, initialized_group: Group=None, *args, **kwargs):
        g = Group(OrderedDict([(35, b'5'), (141, b'Y')]))
        if initialized_group is not None:
            g.merge(initialized_group)
        super().__init__(g, *args, **kwargs)


class NewOrderMessage(MessageWithHeader):

    REQ_TAGS = frozenset((11, 54, 60, 40, 38))
    #REQ_TAGS = frozenset((54, 40, 38))
    REQ_COND = [(lambda self_: self_[40] != b'2' or
                 self_.get(44, None) is not None,
                 'Limit order (40=2) must have price tag (44)'
                 ),
                ]

    def __init__(self, initialized_group: Group=None, **kwargs):
        g = Group({35: b'D'}, req_tags=NewOrderMessage.REQ_TAGS,
                  req_cond=self.REQ_COND)
        if initialized_group is not None:
            g.merge(initialized_group)
        super().__init__(g, **kwargs)

class AmendOrderMessage(MessageWithHeader):

    REQ_TAGS = frozenset((11,41, 54, 60, 40, 38))
    REQ_COND = [(lambda self_: self_[40] != b'2' or
                 self_.get(44, None) is not None,
                 'Limit order (40=2) must have price tag (44)'
                 ),
                ]

    def __init__(self, initialized_group: Group=None, **kwargs):
        g = Group({35: b'G'}, req_tags=AmendOrderMessage.REQ_TAGS,
                  req_cond=self.REQ_COND)
        if initialized_group is not None:
            g.merge(initialized_group)
        super().__init__(g, **kwargs)

class CancelOrderMessage(MessageWithHeader):

    REQ_TAGS = frozenset((11,41, 54,55, 60))
    REQ_COND = [(lambda self_: True,
                 ''
                 ),
                ]

    def __init__(self, initialized_group: Group=None, **kwargs):
        g = Group({35: b'F'}, req_tags=CancelOrderMessage.REQ_TAGS,
                  req_cond=self.REQ_COND)
        if initialized_group is not None:
            g.merge(initialized_group)
        super().__init__(g, **kwargs)


class AckForNewOrderMessage(MessageWithHeader):

      REQ_TAGS = frozenset((11,54,60,40,38,39,150))
      REQ_COND = [(lambda self_: self_.get(150) not in [0,1,2],
                  'Order has not being accepted by Raptor'
                ),
              ]
     
      def __init__(self, initialized_group: Group=None, **kwargs):
          g = Group({35: b'8'}, req_tags=AckForNewOrderMessage.REQ_TAGS,
                    req_cond=self.REQ_COND)
          if initialized_group is not None:
              g.merge(initialized_group)
              super().__init__(g,**kwargs)



class HeartBeatMessage(MessageWithHeader):

    REQ_TAGS = frozenset()
    REQ_COND = []

    def __init__(self, initialized_group: Group=None, **kwargs):
        g = Group({35: b'0'}, req_tags=HeartBeatMessage.REQ_TAGS,
                  req_cond=self.REQ_COND)
        if initialized_group is not None:
            g.merge(initialized_group)
        super().__init__(g, **kwargs)


class TestRequestMessage(MessageWithHeader):

    REQ_TAGS = frozenset()
    REQ_COND = []

    def __init__(self, initialized_group: Group=None, **kwargs):
        g = Group({35: b'1'}, req_tags=TestRequestMessage.REQ_TAGS,
                  req_cond=self.REQ_COND)
        if initialized_group is not None:
            g.merge(initialized_group)
        super().__init__(g, **kwargs)


class SecurityListRequestMessage(MessageWithHeader):

    REQ_TAGS = frozenset((320, 559))
    REQ_COND = []

    def __init__(self, initialized_group: Group=None, **kwargs):
        g = Group({35: b'x'}, req_tags=SecurityListRequestMessage.REQ_TAGS,
                  req_cond=self.REQ_COND)
        if initialized_group is not None:
            g.merge(initialized_group)
        super().__init__(g, **kwargs)


class SecurityListMessage(MessageWithHeader):

    REQ_TAGS = frozenset(())
    REQ_COND = []
    GROUP_STRUCT = GroupStructure(
        {55: GroupStructure({**{x: None for x in (
            55, 48, 107, 561, 167, 22, 207, 541, 200, 202, 201, 
            711, 311, 454, 455, 456, 231, 423, 562, 461, 30025, 
            30024, 30034, 30026, 1205)},
            1206: GroupStructure([1206, 1207, 1208])})})

    def __init__(self, initialized_group: Group=None, **kwargs):
        g = Group({35: b'y'}, req_tags=SecurityListMessage.REQ_TAGS,
                  req_cond=self.REQ_COND)
        if initialized_group is not None:
            g.merge(initialized_group)
        super().__init__(g, **kwargs)

    @classmethod
    def parse(cls, *args, **kwargs):
        if 'init_group' not in kwargs:
            kwargs['init_group'] = SecurityListMessage.GROUP_STRUCT
        return super().parse(*args, **kwargs)
