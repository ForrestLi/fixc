import fix
import nose
from nose.tools import *
import copy
from collections import OrderedDict


class TestGroup():

    def test_group(self):
        g = fix.Group({8: 'oh', 9: 'ah'})
        d = [(k, v) for k, v in g.items()]
        assert d[0][0] == 8
        assert d[0][1] == 'oh'
        assert d[1][0] == 9
        assert d[1][1] == 'ah'

    def test_check_valid_semantics(self):
        g = fix.Group({8: b'8'})
        assert g.is_valid_semantics()
        g = fix.Group({8: 8})
        assert not g.is_valid_semantics()
        g = fix.Group({8: '8'})
        assert not g.is_valid_semantics()
        g = fix.Group({8: fix.Group({1: b'a'})})
        assert not g.is_valid_semantics()
        g = fix.Group({8: [fix.Group({8: b'A'})]})
        assert g.is_valid_semantics()
        g = fix.Group({8: [fix.Group({8: b'A', 9: fix.Group({10: b'1'})})]})
        assert not g.is_valid_semantics()
        g = fix.Group({8: [fix.Group({7: b'A'})]})
        assert not g.is_valid_semantics()

    def test_build(self):
        g_build = fix.Group({8: b'8'}).build()
        assert g_build == b'8=8\x01'

    @raises(ValueError)
    def test_build2(self):
        fix.Group().build()

    def test_id_tag(self):
        g = fix.Group({8: b'8'})
        assert g.id_tag == 8

    def test_deepcopy(self):
        g = fix.Group({8: b'8'})
        g2 = copy.deepcopy(g)
        assert g2 == g
        assert g2 is not g
        g[8] = b'9'
        assert g2 != g

    def test_add_inner_groups(self):
        g = fix.Group(OrderedDict({8: b'FIX v.lol', 350: None}))
        g.add_inner_group(fix.Group({350: b'1', 351: b'22'}))
        g.add_inner_group(fix.Group({350: b'aa', 351: b'bb'}))
        assert g.build() == \
            b'8=FIX v.lol\x01350=1\x01351=22\x01350=aa\x01351=bb\x01'

  #  def y_message(self):
  #      g = fix.Group(OrderedDict({8: b'FIX 4.2', 146: b'2', 55: None}))
  #      g.add_inner_group(fix.Group({55: b'2905', 48: b'2905', 107: b'MAIN RMB RIGHTS', 
  #      561: b'4000', 167: b'CS', 22: b'8', 207: b'HK' ,461: b'ESXXXX', 30025: b'20100812',
  #      30034:b'N'}))
  #      g.add_inner_group(fix.Group({55: b'4068', 48: b'4068', 107: b'TEST~BOND B', 561: b'1000',
  #      167: b'CS', 22: b'8', 207: b'HK', 461: b'DBXXXX', 30025: b'19920601', 30034: b'Y'}))
  #      print('g.build():',g.build())
  #      assert g.build() == \
  #          b'8=FIX.4.2\x01146=2\x0155=2905\x0148=2905\x01107=MAIN RMB RIGHTS\x01561=4000\x01167=CS\x0122=8\x01207=HK\x01461=ESXXXX\x0130025=20100812\x0130034=N\x0155=4068\x0148=4068\x01107=TEST~BOND B\x01561=1000\x01167=CS\x0122=8\x01207=HK\x01461=DBXXXX\x0130025=19920601\x0130034=Y\x01'     
 
    def test_req_tags(self):
        g = fix.Group(OrderedDict({8: b'FIX v.lol', 350: None}),
                      req_tags={8, 350})
        g.add_inner_group(fix.Group({350: b'1', 351: b'22'}, req_tags={351}))
        g.add_inner_group(fix.Group({350: b'aa', 351: b'bb'}))
        g.build()
        del g[8]
        assert_raises(ValueError, g.build)
        g[8] = b'lol'
        g.build()
        del g[350, 0, 351]
        assert_raises(ValueError, g.build)

    def test_req_cond(self):
        g = fix.Group(OrderedDict({8: b'FIX v.lol', 350: None}),
                      req_tags={8, 350}, req_cond=[(lambda self_:
                                                   self_[350][1][351] == b'bb',
                                                    '350, 1, 351 not bb')])
        g.add_inner_group(fix.Group({350: b'1', 351: b'22'}, req_tags={351}))
        g.add_inner_group(fix.Group({350: b'aa', 351: b'bb'}))
        g.build()
        g[350, 1, 351] = b'bbb'
        assert_raises(ValueError, g.build)


class TestGroupStructure():

    def test_groupstructure(self):
        tlg = fix.GroupStructure({350: fix.Group([350, 351])},
                                 validate_construct=False)
        assert not tlg.is_valid_construct()
        tlg = fix.GroupStructure({350: fix.GroupStructure([350, 351])})
        assert tlg.is_valid_construct()
        tlg = fix.GroupStructure({350: 2}, validate_construct=False)
        assert not tlg.is_valid_construct()
        tlg = fix.GroupStructure(
            {350: fix.GroupStructure({350: None, 351: None})})
        assert tlg.is_valid_construct()
        tlg = fix.GroupStructure(
            {350: fix.GroupStructure(
                {350: b'1', 351: None}, validate_construct=False), 341: 1},
            validate_construct=False)
        assert not tlg.is_valid_construct()
   
    def test_y_message_groupstructure(self):    
        tlg = fix.GroupStructure(
        {55: fix.GroupStructure({x: None for x in (
            55, 48, 107, 167, 22, 207, 461, 202, 30025, 30024)})})
        assert tlg.is_valid_construct()
        #tlg = fix.GroupStructure({55: fix.GroupStructure([55, 48, 107, 167, 22, 207, 461, 202, 30025, 30024])})
        #assert tlg.is_valid_construct()


    def test_copy_group_only(self):
        g = fix.GroupStructure({8: None})
        g2 = g.current_level_to_group()
        assert g2[8] is None
        assert g2 is not g
        assert_raises(KeyError, g2.__getitem__, 9)

    @raises(ValueError)
    def test_empty_group(self):
        fix.GroupStructure(validate_construct=True)

    @raises(ValueError)
    def test_empty_group2(self):
        fix.GroupStructure({}, validate_construct=True)

    @raises(ValueError)
    def test_check_valid_construct(self):
        g = fix.GroupStructure({8: b'8'}, req_tags={9})
        assert not g.is_valid_construct()

    @raises(ValueError)
    def test_check_valid_construct2(self):
        g = fix.GroupStructure({})
        g.is_valid_construct()

    def test_inner_groups(self):
        tlg = fix.GroupStructure({350: fix.GroupStructure([350, 351])})
        l = tlg.inner_groups
        assert len(l) == 1
        assert l[0] is tlg[350]

    def test_copy_group(self):
        tlg = fix.GroupStructure({350: fix.GroupStructure([350, 351])})
        g = tlg.current_level_to_group()
        assert dict(g) == {350: None}
        assert g.id_tag == 350

    def test_intersecting_groups(self):
        g = fix.GroupStructure(
            {8: None, 9: None, 34: None,
             350: fix.GroupStructure({350: None, 351: None}),
             351: fix.GroupStructure({351: None}),
             10: None
             }, validate_construct=False)
        assert fix.is_intersecting_groups(g.inner_groups)
