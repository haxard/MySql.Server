using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MySql.Server.Library
{
    
    /// <summary>
    /// A singleton class controlling test database initializing and cleanup
    /// </summary>
    public class MySqlServer : IDisposable
    {
        private static MySqlServer instance;
        public static MySqlServer Instance
        {
            get
            {
                if (instance == null)
                {
                    instance = new MySqlServer(new DBTestConnectionStringFactory());
                }

                return instance;
            }
        }


        private readonly string _mysqlDirectory;
        private readonly string _dataDirectory;
        private readonly string _dataRootDirectory;

        private readonly IDBConnectionStringFactory _conStrFac;

        private MySqlConnection _connection;
        public MySqlConnection Connection
        {
            get
            {
                if (_connection == null)
                    _connection = OpenConnection(_conStrFac.Database());
                if (_connection.State != System.Data.ConnectionState.Open)
                    _connection.Open();
                return _connection;
            }
        }

        private Process _process;

        private MySqlServer(IDBConnectionStringFactory conStrFac)
        {
            _mysqlDirectory = BaseDirHelper.GetBaseDir() + "\\tempServer";
            _dataRootDirectory = _mysqlDirectory + "\\data";
            _dataDirectory = string.Format("{0}\\{1}", _dataRootDirectory, Guid.NewGuid());

            killProcesses();

            createDirs();

            extractMySqlFiles();

            _conStrFac = conStrFac;
        }


        private void createDirs()
        {
            var dirs = new string[] { _mysqlDirectory, _dataRootDirectory, _dataDirectory };

            foreach (string dir in dirs)
            {
                var checkDir = new DirectoryInfo(dir);
                try
                {
                    if (checkDir.Exists)
                    {
                        checkDir.Delete(true);
                    }
                    checkDir.Create();
                }
                catch (Exception)
                {
                    Console.WriteLine("Could not create or delete directory: " + checkDir.FullName);
                }
            }
        }

        private void removeDirs()
        {
            var dirs = new string[] { _mysqlDirectory, _dataRootDirectory, _dataDirectory };

            foreach (string dir in dirs)
            {
                var checkDir = new DirectoryInfo(dir);
                try
                {
                    if (checkDir.Exists)
                    {
                        checkDir.Delete(true);
                    }
                }
                catch (Exception)
                {
                    Console.WriteLine("Could not delete directory: ", checkDir.FullName);
                }
            }
        }

        private void extractMySqlFiles()
        {
            try
            {
                if (!new FileInfo(_mysqlDirectory + "\\mysqld.exe").Exists)
                {
                    File.WriteAllBytes(_mysqlDirectory + "\\mysqld.exe", Properties.Resources.mysqld);
                    File.WriteAllBytes(_mysqlDirectory + "\\errmsg.sys", Properties.Resources.errmsg);
                }
            }
            catch
            {
                throw;
            }
        }

        private static void killProcesses()
        {
            foreach (var process in Process.GetProcessesByName("mysqld"))
            {
                try
                {
                    process.Kill();
                }
                catch (Exception)
                {
                    Console.WriteLine("Tried to kill already existing mysqld process without success");
                }
            }
        }

        public void StartServer()
        {
            _process = new Process();

            var arguments = new[]
            {
                "--standalone",
                "--console",
                string.Format("--basedir=\"{0}\"", _mysqlDirectory),
                string.Format("--lc-messages-dir=\"{0}\"", _mysqlDirectory),
                string.Format("--datadir=\"{0}\"", _dataDirectory),
                "--skip-grant-tables",
                "--enable-named-pipe",

                "--innodb_fast_shutdown=2",
                "--innodb_doublewrite=OFF",
                "--innodb_log_file_size=1048576",
                "--innodb_data_file_path=ibdata1:10M;ibdata2:10M:autoextend"
            };

            _process.StartInfo.FileName = string.Format("\"{0}\\mysqld.exe\"", _mysqlDirectory);
            _process.StartInfo.Arguments = string.Join(" ", arguments);
            _process.StartInfo.UseShellExecute = false;
            _process.StartInfo.CreateNoWindow = true;

            Console.WriteLine(string.Format("Running {0} {1}", _process.StartInfo.FileName, String.Join(" ", arguments)));

            try
            {
                _process.Start();
            }
            catch (Exception e)
            {
                throw new Exception("Could not start server process: " + e.Message);
            }

            waitForStartup();
        }

        /// <summary>
        /// Checks if the server is started. The most reliable way is simply to check if we can connect to it
        /// </summary>
        private void waitForStartup()
        {
            var connected = false;
            var waitTime = 0;

            var lastException = new Exception();

            while (!connected)
            {
                if (waitTime > 10000)
                {
                    throw new Exception("Server could not be started.", lastException);
                }
                waitTime = waitTime + 500;

                try
                {
                    _connection = OpenConnection(_conStrFac.Server());
                    connected = true;
                    ExecuteNonQuery("CREATE DATABASE testserver;USE testserver;", false);
                    Console.WriteLine(string.Format("Database connection established after {0} miliseconds", waitTime));
                }
                catch (Exception e)
                {
                    lastException = e;
                    Thread.Sleep(500);
                    connected = false;
                }
            }
        }

        #region Execute NonQuery
       
        /// <summary>
        /// 
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public async Task ExecuteNonQueryAsync(string query, params KeyValuePair<string, object>[] parameters)
        {
            using (var command = GenerateMySQLCommandFromQuery(query, parameters))
            {
               await ExecuteNonQueryAsync(command);
            }
        }

        /// <summary>
        /// Creates a command from the query and executes it. Supports parameterized queries
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        public void ExecuteNonQuery(string query, params KeyValuePair<string, object>[] parameters)
        {
            var task = ExecuteNonQueryAsync(query, parameters);
            task.Wait();
        }

        /// <summary>
        /// TODO: Make this method static
        /// </summary>
        /// <param name="command"></param>
        /// <param name="useDatabase"></param>
        private async Task ExecuteNonQueryAsync(MySqlCommand command, bool useDatabase = true)
        {
            try
            {
                var connectionString = useDatabase ? _conStrFac.Database() : _conStrFac.Server();
                _connection = OpenConnection(connectionString);
                command.Connection = Connection;
                if (!command.IsPrepared)
                    command.Prepare();
                await command.ExecuteNonQueryAsync();   //TODO: is MySQL.data actually async? https://bugs.mysql.com/bug.php?id=70111
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not execute non query: " + e.Message);
                throw;
            }
            finally
            {
                CloseConnection();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="query"></param>
        /// <param name="useDatabase"></param>
        /// <param name="parameters"></param>
        protected void ExecuteNonQuery(string query, bool useDatabase = true, params KeyValuePair<string, object>[] parameters)
        {
            using (var command = GenerateMySQLCommandFromQuery(query, parameters))
            {
                var task = ExecuteNonQueryAsync(command, useDatabase);
                task.Wait();
            }
        }
#endregion  

        #region Execute Reader
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public async Task<MySqlDataReader> ExecuteReaderAsync(string query, params KeyValuePair<string, object>[] parameters)
        {
            using (var command = GenerateMySQLCommandFromQuery(query, parameters))
            {
                return await ExecuteReaderAsync(command);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        public MySqlDataReader ExecuteReader(string query, params KeyValuePair<string, object>[] parameters)
        {
            var task = ExecuteReaderAsync(query, parameters);
            task.Wait();
            return task.Result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="command"></param>
        /// <returns></returns>
        private async Task<MySqlDataReader> ExecuteReaderAsync(MySqlCommand command)
        {
            try
            {
                command.Connection = Connection; 
                if (!command.IsPrepared)
                    command.Prepare();
                return await Task.Run(() => command.ExecuteReader()); //TODO: is MySQL.data actually async? https://bugs.mysql.com/bug.php?id=
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not execute query: " + e.Message);
                throw;
            }
        }
        #endregion

        #region Execute Scalar

        /// <summary>
        /// 
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public async Task<object> ExecuteScalarAsync(string query, params KeyValuePair<string, object>[] parameters)
        {
            using (var command = GenerateMySQLCommandFromQuery(query, parameters))
            {
                return await ExecuteScalarAsync(command);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        public object ExecuteScalar(string query, params KeyValuePair<string, object>[] parameters)
        {
            var task = ExecuteScalarAsync(query, parameters);
            task.Wait();
            return task.Result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="command"></param>
        /// <returns></returns>
        private async Task<object> ExecuteScalarAsync(MySqlCommand command)
        {
            try
            {
                command.Connection = Connection;
                if (!command.IsPrepared)
                    command.Prepare();
                return await command.ExecuteScalarAsync(); //TODO: is MySQL.data actually async? https://bugs.mysql.com/bug.php?id=70111
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not execute query: " + e.Message);
                throw;
            }
        }
        #endregion

        /// <summary>
        /// Takes a query and returns a MySQLCommand. Supports parameterized queries
        /// </summary>
        /// <param name="query">The query text</param>
        /// <param name="parameters">Collection of parameters</param>
        /// <returns>A MySQLCommand which can be executed against a database connection</returns>
        private static MySqlCommand GenerateMySQLCommandFromQuery(string query, params KeyValuePair<string, object>[] parameters)
        {
            var command = new MySqlCommand(query);
            foreach (KeyValuePair<string, object> kvp in parameters)
                command.Parameters.AddWithValue(kvp.Key, kvp.Value);
            return command;
        }

        private MySqlConnection OpenConnection(string connectionString)
        {
            MySqlConnection conn = new MySqlConnection(connectionString);
            conn.Open();
            return conn;
        }

        public void CloseConnection()
        {
            if (_connection.State != System.Data.ConnectionState.Closed)
                _connection.Close();
        }

        public void ShutDown()
        {
            try
            {
                CloseConnection();
                if (!_process.HasExited)
                {
                    _process.Kill();
                }

                _process.Dispose();
                _process = null;
                killProcesses();
                removeDirs();
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not close database server process: " + e.Message);
                throw;
            }
        }

        private bool disposed;
        public void Dispose()
        {
            Dispose(true);
        }
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
            {
                return;
            }
            if (disposing)
            {
                CloseConnection();
                _connection.Dispose();
                if (instance != null)
                {
                    instance.Dispose();
                    instance = null;
                }
                if (_process != null)
                {
                    _process.Dispose();
                    _process = null;
                }
            }

            disposed = true;
        }
    }
}
