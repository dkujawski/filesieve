import datetime
import os
import shutil

from nose.tools import assert_equal
from nose.tools import assert_false
from nose.tools import assert_true

from filesieve import sieve

class TestSieve(object):
    def setup(self):
        test_dir = os.path.abspath(os.path.dirname(__file__))        
        stamp = datetime.datetime.now().strftime('%Y%m%d%H%M%s')
        self.base = os.path.join(test_dir, stamp)
        self.src = os.path.join(self.base, 'src')
        self.dup = os.path.join(self.base, 'dup')
        os.makedirs(self.dup)
        data = os.path.join(test_dir, 'data')
        shutil.copytree(data, self.src)
        return    
    def teardown(self):
        if os.path.exists(self.base):
            shutil.rmtree(self.base)
        return
    def test_walk(self):
        s = sieve.Sieve()
        s.dup_dir = self.dup
        b = s.walk(self.src)
        d = s.dup_count
        # TODO: fix this test so that it actually checks the diff between the
        # files. it is not deterministic which file will be moved or found first 
        expected = {
          '787ada88e6c442bb3ec6b30c97b9126c': os.path.join(self.src, 'big_diff.log'), 
          'c86eaa9d51d51dfe1a6a404739f62303': os.path.join(self.src, 'small_diff.log'), 
          '5819b7a15d098be2c28f04e6edfb7515': os.path.join(self.src, 'big_copy.log'), 
          'ca77696740831b2ac340f71140e641cb': os.path.join(self.src, 'small_copy.log'),
        }
        assert_equal(b, expected)
        assert_equal(d, 2)
        return
    def test_clean_dup(self):
        dup_file = os.path.join(self.src, 'small_copy.log')
        sieve.clean_dup(dup_file, self.dup)
        expected = os.path.join(self.dup, dup_file.lstrip('/'))
        assert_true(os.path.exists(expected))
        assert_false(os.path.exists(dup_file))
        return
    
class TestSieveStaticFuncs(object):
    def setup(self):
        test_dir = os.path.abspath(os.path.dirname(__file__))
        self.data_dir = os.path.join(test_dir, 'data')
        return
    def teardown(self):
        return    
    def test_get_hash_key(self):
        expected = "e4578cd35d06171139bad5b66adca0fc"
        fp = os.path.join(self.data_dir, 'small_orig.log')
        with open(fp) as fh:
            data = fh.read()
        found = sieve.get_hash_key(data)
        assert_equal(expected, found)
        return
    
    