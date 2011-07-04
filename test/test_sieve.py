import datetime
import os
import shutil

from nose.tools import assert_equal

from filesieve import sieve

class TestSieve(object):
    def __init__(self):
        self.test_dir = os.path.abspath(os.path.dirname(__file__))
        return
    def setup(self):        
        stamp = datetime.datetime.now().strftime('%Y%m%d%H%M%s')
        self.base = os.path.join(self.test_dir, stamp)
        self.src = os.path.join(self.base, 'src')
        self.dup = os.path.join(self.base, 'dup')
        os.makedirs(self.dup)
        data = os.path.join(self.test_dir, 'data')
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
    
class TestSieveStaticFuncs(object):
    def setup(self):
        return
    def teardown(self):
        return    
    def test_get_hash_key(self):
        return
    def test_clean_dup(self):
        return
    
if __name__ == '__main__':
    print 'testing....\n'
    
    ts = TestSieve()
    ts.setup()
    ts.test_walk()
    ts.teardown()
    
    print '\ndone!'