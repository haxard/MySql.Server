using System;

namespace MySql.Server.Library
{
    internal class DBTestConnectionStringFactory : IDBConnectionStringFactory
    {
        public string Server()
        {
            return "Server=" + "127.0.0.1" + ";Protocol=pipe;";
        }


        public string Database()
        {
            return Database("testserver");
        }

        /**
         * Returns a connection string of a specific database
         */
        public string Database(string databaseName)
        {
            return string.Format("Server=127.0.0.1;Database={0};Protocol=pipe;", databaseName);
        }
    }
}
