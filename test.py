import unittest
from HelloFresh import HelloFresh

class Tests(unittest.TestCase):
    def test1_getRequest(self):
        global obj
        self.assertRaises(Exception, HelloFresh, "Random Text as URL")
        self.assertRaises(Exception, HelloFresh, "https://www.google.com/")
        obj = HelloFresh("https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json")
        self.assertIsInstance(obj, HelloFresh)

    def test2_initializeValues(self):
        self.assertEqual(obj.easyAvgTime, 0)
        self.assertEqual(obj.mediumAvgTime, 0)
        self.assertEqual(obj.hardAvgTime, 0)
        self.assertEqual(obj.easyTime, 0)
        self.assertEqual(obj.hardTime, 0)
        self.assertEqual(str(type(obj.df)), "<class 'pyspark.sql.dataframe.DataFrame'>")

    def test3_setConfig(self):
        self.assertTrue(obj.config(easy=0, hard=0))
        self.assertEqual(obj.easyTime, 0)
        self.assertEqual(obj.hardTime, 0)
        self.assertTrue(obj.config(easy=30, hard=60))
        self.assertEqual(obj.easyTime, 30)
        self.assertEqual(obj.hardTime, 60)
        self.assertFalse(obj.config(easy=-30, hard=-60))
        self.assertNotEqual(obj.easyTime, -30)
        self.assertNotEqual(obj.hardTime, -60)
        self.assertEqual(obj.easyTime, 30)
        self.assertEqual(obj.hardTime, 60)

    def test4_getDataFrame(self):
        self.assertRaises(TypeError,obj.get)
        self.assertFalse(obj.get(""))
        self.assertTrue(obj.get("beef"))
        self.assertNotEqual(obj.easyAvgTime, 0)
        self.assertNotEqual(obj.mediumAvgTime, 0)
        self.assertNotEqual(obj.hardAvgTime, 0)

    def test5_printString(self):
        self.assertIsNotNone(str(obj))

    def test6_saveReport(self):
        self.assertRaises(TypeError,obj.save)
        self.assertRaises(FileNotFoundError,obj.save,"")
        self.assertTrue(obj.save("output/report.csv"))        

if __name__=='__main__':
      unittest.main()