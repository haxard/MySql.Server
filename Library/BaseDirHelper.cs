using System;
using System.Collections.Generic;
using System.IO;

namespace MySql.Server.Library
{
    internal class BaseDirHelper
    {
        private static string baseDir;
        public static string GetBaseDir()
        {
            if (baseDir == null)
            {
                baseDir = new DirectoryInfo(Directory.GetCurrentDirectory()).Parent.Parent.FullName;
            }

            return baseDir;
        }
    }
}
