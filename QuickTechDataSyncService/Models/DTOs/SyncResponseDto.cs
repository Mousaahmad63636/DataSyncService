using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Models.DTOs
{
    public class SyncResponseDto<T> where T : class
    {
        public bool Success { get; set; }
        public string Message { get; set; } = string.Empty;
        public List<T> Data { get; set; } = new List<T>();
        public int TotalCount { get; set; }
        public int PageCount { get; set; }
        public int CurrentPage { get; set; }
        public DateTime SyncTime { get; set; } = DateTime.UtcNow;
        public bool HasMorePages => CurrentPage < PageCount;
    }
}