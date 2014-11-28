using System;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

namespace DynamicPivot
{
    public class UserDefindedFunctions
    {
        [SqlProcedure(Name = "ClrDynamicPivot")]
        public static void ClrDynamicPivot(SqlString query, SqlString pivotColumn, SqlString selectCols, SqlString aggCols, SqlString orderBy)
        {
            var stmt = string.Empty;
            try
            {
                CreateTempTable(query);
                var pivot = GetPivotData(pivotColumn.ToString());
                stmt = string.Format("select * from ( select {0} from #temp ) as t pivot ( {1} for {2} in ( {3} )) as p {4}",
                    selectCols,
                    aggCols,
                    pivotColumn,
                    pivot,
                    orderBy);
                using (var cn = new SqlConnection("Context Connection=True"))
                {
                    var cmd = cn.CreateCommand();
                    cmd.CommandText = stmt;
                    cn.Open();
                    var reader = cmd.ExecuteReader();
                    if (SqlContext.Pipe != null)
                        SqlContext.Pipe.Send(reader);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("clrDynamicPivot Error stmt:{0}", stmt), ex);
            }
        }

        public static void CreateTempTable(SqlString query)
        {
            using (var sqlconn = new SqlConnection("Context Connection=True"))
            {
                var sqlCmd = sqlconn.CreateCommand();
                sqlCmd.CommandText = query.ToString();
                sqlconn.Open();
                sqlCmd.ExecuteNonQuery();
            }
        }

        public static string GetPivotData(string pivotColumn)
        {
            var stmt = string.Format("select distinct {0} from #temp", pivotColumn);
            var pivotCols = string.Empty;

            using (var cn = new SqlConnection("Context Connection=True"))
            {
                var cmd = cn.CreateCommand();
                cmd.CommandText = stmt;
                cn.Open();
                using (var dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                    {
                        if (dr.GetFieldType(0) == typeof(Int32))
                            pivotCols += "[" + dr.GetInt32(0) + "],";
                        if (dr.GetFieldType(0) == typeof(Decimal))
                            pivotCols += "[" + dr.GetDecimal(0) + "],";
                        if (dr.GetFieldType(0) == typeof(String))
                            pivotCols += "[" + dr.GetString(0) + "],";
                    }
                }
            }
            return pivotCols.Remove(pivotCols.Length - 1);
        }
    }
}
