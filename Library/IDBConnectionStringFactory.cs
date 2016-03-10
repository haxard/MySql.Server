using System;
using System.Collections.Generic;
using System.Linq;

namespace MySql.Server.Library
{
    internal interface IDBConnectionStringFactory
    {
        string Server();
        string Database();
        string Database(string databaseName);
    }
}
