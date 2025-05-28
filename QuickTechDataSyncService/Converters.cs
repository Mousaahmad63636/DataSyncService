using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;

namespace QuickTechDataSyncService
{
    public class StatusToBrushConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is string status)
            {
                return status.ToLower() switch
                {
                    "running" => new SolidColorBrush(Colors.Green),
                    "stopped" => new SolidColorBrush(Colors.Gray),
                    "error" => new SolidColorBrush(Colors.Red),
                    "connected" => new SolidColorBrush(Colors.Green),
                    "disconnected" => new SolidColorBrush(Colors.Gray),
                    _ => new SolidColorBrush(Colors.Orange)
                };
            }
            return new SolidColorBrush(Colors.Gray);
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    public class ContainsErrorConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is string text)
            {
                return text.Contains("ERROR", StringComparison.OrdinalIgnoreCase) ||
                       text.Contains("EXCEPTION", StringComparison.OrdinalIgnoreCase) ||
                       text.Contains("FAILED", StringComparison.OrdinalIgnoreCase);
            }
            return false;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    public class ContainsWarningConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is string text)
            {
                return text.Contains("WARNING", StringComparison.OrdinalIgnoreCase) ||
                       text.Contains("WARN", StringComparison.OrdinalIgnoreCase);
            }
            return false;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    public class ContainsSuccessConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is string text)
            {
                return text.Contains("SUCCESS", StringComparison.OrdinalIgnoreCase) ||
                       text.Contains("STARTED SUCCESSFULLY", StringComparison.OrdinalIgnoreCase);
            }
            return false;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}