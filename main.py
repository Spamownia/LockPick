using System;
using MySql.Data.MySqlClient;

namespace SCUMBot
{
    class DBConnect
    {
        // Connection string pod Twoją bazę FreeSQLDatabase
        public string connectionString = "SERVER=sql7.freesqldatabase.com;DATABASE=sql7808250;UID=sql7808250;PASSWORD=TPYRVWM2Lx;PORT=3306;";
        private MySqlConnection connection;

        public DBConnect()
        {
            Initialize();
        }

        private void Initialize()
        {
            connection = new MySqlConnection(connectionString);
        }

        // Tymczasowa funkcja testowa połączenia
        public void TestConnection()
        {
            if (OpenConnection())
            {
                Console.WriteLine("Połączono z bazą!");
                CloseConnection();
            }
            else
            {
                Console.WriteLine("Błąd połączenia.");
            }
        }

        /*
        // Przykład pobrania jednego zamówienia
        public string[] get_order()
        {
            string[] data = { "ERROR", "" };

            if (this.OpenConnection() == true)
            {
                try
                { 
                    var cmd = connection.CreateCommand();
                    cmd.CommandText = "SELECT id, player_id, item_id FROM orders ORDER BY created_at LIMIT 1";

                    var reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        data = new string[] { reader.GetString(0), reader.GetString(1), reader.GetString(2) };
                    }
                }
                finally
                {
                    CloseConnection();
                }
            }

            return data;
        }
        */

        private bool OpenConnection()
        {
            try
            {
                connection.Open();
                return true;
            }
            catch (MySqlException ex)
            {
                switch (ex.Number)
                {
                    case 0:
                        Console.WriteLine("Nie można połączyć się z serwerem.");
                        break;

                    case 1045:
                        Console.WriteLine("Niepoprawny użytkownik/hasło.");
                        break;
                }
                return false;
            }
        }

        private bool CloseConnection()
        {
            try
            {
                connection.Close();
                return true;
            }
            catch (MySqlException ex)
            {
                Console.WriteLine("Błąd przy zamykaniu połączenia: " + ex.Message);
                return false;
            }
        }
    }
}
