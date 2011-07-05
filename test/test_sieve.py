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
        b,d = s.walk(self.src)
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
        expected = "f6325473c8d80f37cf5711c1e97e92f0"
        data = os.path.join(self.data_dir, 'small_orig.log')
        found = sieve.get_hash_key(data)
        assert_equal(expected, found)
        return
    
if __name__ == '__main__':
    print 'testing....\n'
    
    ts = TestSieve()
    ts.setup()
    ts.test_walk()
    ts.teardown()
    
    ts.setup()
    ts.test_clean_dup()
    ts.teardown()

    tssf = TestSieveStaticFuncs()
    tssf.setup()
    tssf.test_get_hash_key()
    tssf.teardown()
    
    print '\ndone!'