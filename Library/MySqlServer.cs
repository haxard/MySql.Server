using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

namespace MySql.Server.Library
{
    /**
     * A singleton class controlling test database initializing and cleanup
     */
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

        private MySqlConnection _myConnection;
        public MySqlConnection Connection
        {
            get
            {
                if (_myConnection == null)
                {
                    OpenConnection(_conStrFac.Database());
                }
                return _myConnection;
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

        /**
         * Checks if the server is started. The most reliable way is simply to check
         * if we can connect to it
         **/
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
                    OpenConnection(_conStrFac.Server());
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

        private void ExecuteNonQuery(string query, bool useDatabase)
        {
            var connectionString = useDatabase ? _conStrFac.Database() : _conStrFac.Server();
            OpenConnection(connectionString);
            try
            {
                using (var command = new MySqlCommand(query, _myConnection))
                {
                    command.ExecuteNonQuery();
                }
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

        public void ExecuteNonQuery(string query)
        {
            ExecuteNonQuery(query, true);
        }

        public MySqlDataReader ExecuteReader(string query)
        {
            OpenConnection(_conStrFac.Database());

            try
            {
                using (var command = new MySqlCommand(query, _myConnection))
                {
                    return command.ExecuteReader();
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        private void OpenConnection(string connectionString)
        {
            if (_myConnection == null)
            {
                _myConnection = new MySqlConnection(connectionString);
            }

            if (_myConnection.State != System.Data.ConnectionState.Open)
            {
                _myConnection.Open();
            }
        }

        public void CloseConnection()
        {
            if (_myConnection.State != System.Data.ConnectionState.Closed)
            {
                _myConnection.Close();
            }
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
                _myConnection.Dispose();
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
