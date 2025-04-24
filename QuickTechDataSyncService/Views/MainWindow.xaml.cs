using System.Windows;
using QuickTechDataSyncService.ViewModels;

namespace QuickTechDataSyncService
{
    public partial class MainWindow : Window
    {
        private readonly MainViewModel _viewModel;

        public MainWindow()
        {
            InitializeComponent();
            _viewModel = new MainViewModel();
            DataContext = _viewModel;
        }

        private async void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            if (_viewModel.IsServerRunning)
            {
                e.Cancel = true;
                await ShutdownGracefully();
                Application.Current.Shutdown();
            }
        }

        private async Task ShutdownGracefully()
        {
            await _viewModel.ShutdownAsync();
        }
    }
}