using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace MySql.Server
{
    [TestClass]
    public class Example
    {
        [AssemblyInitialize]
        public static void Initialize(TestContext context)
        {
            var dbServer = MySql.Server.Library.MySqlServer.Instance;
            dbServer.StartServer();


            dbServer.ExecuteNonQuery("CREATE TABLE testTable (`id` INT NOT NULL, `value` CHAR(150) NULL,  PRIMARY KEY (`id`)) ENGINE = MEMORY;");


            dbServer.ExecuteNonQuery("INSERT INTO testTable (`value`) VALUES ('some value')");
        }


        [AssemblyCleanup]
        public static void Cleanup()
        {
            var dbServer = MySql.Server.Library.MySqlServer.Instance;

            dbServer.ShutDown();
        }


        [TestMethod]
        public void TestMethod()
        {
            var database = MySql.Server.Library.MySqlServer.Instance;

            database.ExecuteNonQuery("insert into testTable (`id`, `value`) VALUES (2, 'test value')");

            using (var reader = database.ExecuteReader("select * from testTable WHERE id = 2"))
            {
                reader.Read();

                Assert.AreEqual("test value", reader.GetString("value"), "Inserted and read string should match");
            }
        }
    }
}
