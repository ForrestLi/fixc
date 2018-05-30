import fix
import nose
from nose.tools import *


class TestMessage2():

    def setup(self):
        self.content_part = b'34=1\x01350=1\x01351=22\x0177=1\x01351=oh\x0177=2\x01350=aa\x01351=bb\x0177=3\x01351=my\x0177=4\x01'
        self.header = \
            b'8=FIX v.lol\x019=' + \
            str(len(self.content_part)).encode() + b'\x01'
        self.checksum = b'10=' + \
            str(sum(self.content_part + self.header) % 256).zfill(3).encode()
        self.byte_msg = self.header + self.content_part + self.checksum + \
            b'\x01'

    @raises(fix.RepeatedTagError)
    def test_parse2(self):
        fix.Message.parse(self.byte_msg)

    def test_parse(self):
        tlg = fix.GroupStructure({350: fix.GroupStructure({350: None, 351: fix.GroupStructure([351, 77])})})
        m = fix.Message.parse(self.byte_msg, tlg)
        assert m[8] == b'FIX v.lol'
        assert m[34] == b'1'
        assert m[350, 0, 351, 1, 77] == b'2'
        expected_checksum = str((sum(self.content_part) +
                                 sum(self.header)) % 256).zfill(3).encode()
        assert m.checksum == expected_checksum
        inner_group_id_tags = set(m.iter_inner_group_id_tags())
        assert inner_group_id_tags == {350}


class TestMessage():

    def setup(self):
        self.content_part = b'34=1\x01350=1\x01351=22\x01350=aa\x01351=bb\x01'
        self.header = \
            b'8=FIX v.lol\x019=' + \
            str(len(self.content_part)).encode() + b'\x01'
        self.checksum = b'10=' + \
            str(sum(self.content_part + self.header) % 256).zfill(3).encode()
        self.byte_msg = self.header + self.content_part + self.checksum + \
            b'\x01'

    @raises(fix.RepeatedTagError)
    def test_parse2(self):
        fix.Message.parse(self.byte_msg)

    def test_parse(self):
        tlg = fix.GroupStructure({350: fix.GroupStructure([350, 351])})
        m = fix.Message.parse(self.byte_msg, tlg)
        assert m[8] == b'FIX v.lol'
        assert m[34] == b'1'
        assert m[350][0][350] == b'1'
        assert m[350, 0, 350] == b'1'
        assert m[350][0][351] == b'22'
        assert m[350][1][350] == b'aa'
        assert m[350][1][351] == b'bb'
        expected_checksum = str((sum(self.content_part) +
                                 sum(self.header)) % 256).zfill(3).encode()
        assert m.checksum == expected_checksum
        inner_group_id_tags = set(m.iter_inner_group_id_tags())
        assert inner_group_id_tags == {350}

    def test_build(self):
        tlg = fix.GroupStructure({350: fix.GroupStructure([350, 351])})
        m = fix.Message.parse(self.byte_msg, tlg)
        build = bytes(m)
        assert build == self.byte_msg

    def test_check_semantics(self):
        tlg = fix.GroupStructure({350: fix.GroupStructure([350, 351])})
        m = fix.Message.parse(self.byte_msg, tlg)
        assert m.is_valid_semantics()

        m[34] = 1
        assert not m.is_valid_semantics()

    def test_check_header_trailer(self):
        tlg = fix.GroupStructure({350: fix.GroupStructure([350, 351])})
        m = fix.Message.parse(self.byte_msg, tlg)
        assert m.is_valid_header_trailer()

        m[34] = b'2'
        assert not m.is_valid_header_trailer()

        m[34] = b'22'
        assert not m.is_valid_header_trailer()

        m[34] = b'1'
        del m[350][0][350]
        assert not m.is_valid_header_trailer()

    def test_modify(self):
        tlg = fix.GroupStructure({350: fix.GroupStructure([350, 351])})
        m = fix.Message.parse(self.byte_msg, tlg)
        m[34] = b'2'
        m[350, 1, 350] = b'cc'
        del m[350, 0, 351]
        build = bytes(m)
        expected = self.byte_msg.replace(b'34=1', b'34=2').replace(
            b'350=aa', b'350=cc').replace(b'351=22\x01', b'')
        assert build == expected

    def test_checksum(self):
        tlg = fix.GroupStructure({350: fix.GroupStructure([350, 351])})
        m = fix.Message.parse(self.byte_msg, tlg)
        assert m.is_valid_checksum()
        m[34] = b'0'
        assert not m.is_valid_checksum()
        m[10] = str(int(m[10].decode()) - 1).encode().zfill(3)
        assert m.is_valid_checksum()

    def test_reset(self):
        tlg = fix.GroupStructure({350: fix.GroupStructure([350, 351])})
        m = fix.Message.parse(self.byte_msg, tlg)
        m.reset(seqnum=1)
        assert m.is_valid_semantics()
        m.reset(seqnum=2, extra={8: b'2'})
        assert m.is_valid_semantics()

    def test_auto_reset(self):
        tlg = fix.GroupStructure({350: fix.GroupStructure([350, 351])})
        m = fix.Message.parse(self.byte_msg, tlg, auto_reset=True)
        assert m.is_valid_semantics()
        m[8] = b'lol'
        assert m.is_valid_semantics()
        del m[9]
        assert m.is_valid_semantics()


class TestMessageWithHeader():

    def setup(self):
        self.msg = fix.MessageWithHeader(fix.Group(
                {35: b'D', 49: b'S', 56: b'T', 34: b'1', 999: b'tag 999 value'}
            ))
        self.msg2 = fix.MessageWithHeader(fix.Group(
                {35: b'D', 49: b'S', 56: b'T', 34: b'1', 50: b'lol',
                 999: b'tag 999 value'}
            ))

    def test_valid(self):
        assert self.msg.is_valid_semantics()

    def test_init(self):
        build = bytes(self.msg)
        tags = {int(t.decode()) for t, v in fix.util.iter_rawmsg(build)}
        expected = {98, 35, 34, 999, 8, 9, 10, 11, 108, 49, 52, 56, 60}
        assert tags == expected
        build = bytes(self.msg2)
        tags = {int(t.decode()) for t, v in fix.util.iter_rawmsg(build)}
        expected = {98, 35, 34, 999, 8, 9, 10, 11, 108, 49, 50, 52, 56, 60}
        assert tags == expected

    def test_valid_cond(self):
        assert self.msg.is_valid_cond()
        assert self.msg2.is_valid_cond()
        bl = self.msg[9]
        self.msg[9] = b'99999'
        assert not self.msg.is_valid_cond()
        del self.msg[9]
        assert not self.msg.is_valid_cond()
        self.msg[9] = bl  # appended to end
        assert not self.msg.is_valid_cond()
        del self.msg2[34]
        assert not self.msg2.is_valid_cond()


class TestNewOrderMessage():

    def setup(self):
        self.msg = fix.NewOrderMessage(
            fix.Group({38: b'100', 40: b'2', 44: b'1', 54: b'0', 55: b'fja',
                       49: b'S', 56: b'T', 34: b'1'})
        )

    def test_init(self):
        pass

    def test_auto_reset(self):
        self.msg.auto_reset = True
        self.msg.qty = 200
        assert self.msg.is_valid_semantics()
