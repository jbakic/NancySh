using System;
using System.Xml.Serialization;
using System.IO;

namespace nancySh
{
    public class ServerConfig
    {
        public Server[] Servers;

        public static ServerConfig Load(string file)
        {
            var serializer = new XmlSerializer(typeof(ServerConfig));
            return (ServerConfig)serializer.Deserialize(File.OpenRead(file));
        }
    }

    public class Server
    {
        [XmlAttribute]
        public int Id;
        [XmlAttribute]
        public string BaseUrl;

        public string BackupDbConnString;
    }
}

