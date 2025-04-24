using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Models
{
    public class SyncLog
    {
        public int Id { get; set; }
        public string DeviceId { get; set; } = string.Empty;
        public string EntityType { get; set; } = string.Empty;
        public DateTime LastSyncTime { get; set; }
        public bool IsSuccess { get; set; }
        public string? ErrorMessage { get; set; }
        public int RecordsSynced { get; set; }
    }
}