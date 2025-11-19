import { Component, OnInit } from '@angular/core';
import { FetchFiles, OpenInputFileDialog, OpenOutputDirectoryDialog, RunCLIFetch } from '../../wailsjs/go/app/App';


@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit {
  status = 'Ready';
  inputFilePath = '';
  outputDirPath = '';

  // Global output logs that appear in the Output panel
  outputLogs: string[] = [];

  // Advanced options / UI state
  showAdvanced = false;
  maxConnections = 8;
  maxRetries = 3;
  simultaneousDownloads = 2;
  skipExisting = true;
  downloadInParallel = true;

  // Collapse state
  filesCollapsed = false;
  settingsCollapsed = true;
  outputCollapsed = true;

  // Dark mode
  isDarkMode = false;

  // Overall download progress
  overallProgress = 0;

  // Per-source progress model
  sources: Array<{
    id: string;
    title: string;
    progress: number;
    accent: string;
    logs: string[];
    status?: string;
  }> = [];

  ngOnInit() {
    // Detect system theme preference
    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
      this.isDarkMode = true;
    }

    // Listen for system theme changes
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
      this.isDarkMode = e.matches;
    });

    // Example sources
    this.sources = [
      {
        id: 'src-1',
        title: 'Source 1',
        progress: 100,
        accent: '#4caf50',
        logs: ['Connecting…', 'Downloading series 1/5', 'Chunk 32/120', 'Writing file 00000001.dcm', 'Writing file 00000002.dcm', 'Rate 12.5 MB/s', 'ETA 01:45', 'Rate 12.5 MB/s', 'ETA 01:45','Rate 12.5 MB/s', 'ETA 01:45','Rate 12.5 MB/s', 'ETA 01:45','Rate 12.5 MB/s', 'ETA 01:45','Rate 12.5 MB/s', 'ETA 01:45','Rate 12.5 MB/s', 'ETA 01:45','Rate 12.5 MB/s', 'ETA 01:45','Rate 12.5 MB/s', 'ETA 01:45','Rate 12.5 MB/s', 'ETA 01:45','Rate 12.5 MB/s', 'ETA 01:45','Rate 12.5 MB/s', 'ETA 01:45','Rate 12.5 MB/s', 'ETA 01:45'],
        status: 'downloading'
      },
      {
        id: 'src-2',
        title: 'Source 2',
        progress: 20,
        accent: '#ff9800',
        logs: ['Queued…', 'Preparing download', 'Resolving metadata', 'Starting…'],
        status: 'queued'
      },
      {
        id: 'src-3',
        title: 'Source 3',
        progress: 60,
        accent: '#3f51b5',
        logs: ['Downloading…', 'File 10/200', 'Rate 8.3 MB/s', 'ETA 02:14'],
        status: 'downloading'
      }
    ];

    this.updateOverallProgress();
  }

  toggleDarkMode() {
    this.isDarkMode = !this.isDarkMode;
  }

  onSelectOutputDirectory() {
    OpenOutputDirectoryDialog().then((dirPath: string) => {
      if (dirPath) {
        this.outputDirPath = dirPath;
      }
    }).catch(err => {
      this.status = "Error: " + err;
    });
  }

  onFetchFiles() {
    if (!this.inputFilePath || !this.outputDirPath) {
      this.status = "Please select an input TCIA file, an output directory, and a Manifests directory.";
      return;
    }

    // Reconstruct the exact CLI command for display (quote paths to handle spaces)
    const cliPath = '../nbia-data-retriever-cli';
    const parts: string[] = [];
    parts.push(cliPath);
    parts.push('-i');
    parts.push(`"${this.inputFilePath}"`);
    parts.push('--output');
    parts.push(`"${this.outputDirPath}"`);
    parts.push('--max-connections');
    parts.push(String(this.maxConnections));
    parts.push('--max-retries');
    parts.push(String(this.maxRetries));
    parts.push('--processes');
    parts.push(String(this.simultaneousDownloads));
    if (this.downloadInParallel) {
      parts.push('--download-in-parallel');
    }
    if (this.skipExisting) {
      parts.push('--skip-existing');
    }
    const cmdStr = parts.join(' ');

    // Show command immediately in the status window
    this.status = 'Running: ' + cmdStr;
    this.appendLog(this.status);

    // Call backend to run the CLI
    RunCLIFetch(this.inputFilePath, this.outputDirPath, this.maxConnections, this.maxRetries, this.simultaneousDownloads, this.skipExisting, this.downloadInParallel)
      .then((result: string) => {
        this.status = result;
        this.appendLog(result);
      })
      .catch(err => {
        this.status = "Error: " + err;
        this.appendLog(this.status);
      });
  }

  onSelectInputFile() {
    OpenInputFileDialog().then((filePath: string) => {
      if (filePath) {
        this.inputFilePath = filePath;
      }
    }).catch(err => {
      this.status = "Error: " + err;
    });
  }

  // Helpers for backend integration
  setSources(sources: Array<{ id: string; title: string; progress: number; accent: string; logs: string[]; status?: string; }>) {
    this.sources = sources || [];
    this.updateOverallProgress();
  }

  // Update progress for a single source by id
  updateSourceProgress(id: string, progress: number) {
    const s = this.sources.find(x => x.id === id);
    if (s) {
      s.progress = Math.max(0, Math.min(100, Math.round(progress)));
      this.updateOverallProgress();
    }
  }

  // Append a log line to a specific source
  appendSourceLog(id: string, line: string) {
    const s = this.sources.find(x => x.id === id);
    if (s) {
      s.logs.push(line);
    }
  }

  // Append to the global output panel
  appendLog(line: string) {
    this.outputLogs.push(line);
  }

  // Calculate Overall Progress
  updateOverallProgress() {
    const list = this.sources ?? [];
    let sum = 0;
    for (const s of list) sum += (s.progress ?? 0);
    this.overallProgress = list.length ? Math.round(sum / list.length) : 0;
  }
}
