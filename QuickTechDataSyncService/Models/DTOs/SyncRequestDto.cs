using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Models.DTOs
{
    public class SyncRequestDto
    {
        public string DeviceId { get; set; } = string.Empty;
        public DateTime? LastSyncTime { get; set; }
        public string EntityType { get; set; } = string.Empty;
        public int PageSize { get; set; } = 100;
        public int PageNumber { get; set; } = 1;
    }
}