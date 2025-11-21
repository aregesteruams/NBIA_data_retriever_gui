import { Component, OnInit, OnDestroy, ElementRef, ViewChild } from '@angular/core';
import { EventsOn, EventsOff } from '../../wailsjs/runtime/runtime';
import { FetchFiles, OpenInputFileDialog, OpenOutputDirectoryDialog, RunCLIFetch, CancelRun } from '../../wailsjs/go/app/App';

// The GUI backend exposes SetAccessToken/ClearAccessToken methods. The generated
// Wails bindings may not be present in some dev environments, so declare them
// here to avoid type errors and call them at runtime.
declare function SetAccessToken(token: string): Promise<void>;
declare function ClearAccessToken(): Promise<void>;


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
  @ViewChild('outputContainer') outputContainer!: ElementRef;

  private unsubscribeRuntime?: () => void;

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

  // Running state
  isRunning = false;

  // Access token input (optional)
  accessTokenInput = '';

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

    // Subscribe to streaming CLI output from backend
    this.unsubscribeRuntime = EventsOn('cli-output-line', (line: string) => {
      // Append line to global output logs
      this.outputLogs.push(line);
      // Auto-scroll
      setTimeout(() => {
        try {
          const el = this.outputContainer?.nativeElement as HTMLElement;
          if (el) el.scrollTop = el.scrollHeight;
        } catch (e) {
          // ignore
        }
      }, 10);
    });
  }

  ngOnDestroy() {
    if (this.unsubscribeRuntime) this.unsubscribeRuntime();
    try { EventsOff('cli-output-line'); } catch (e) { /* ignore */ }
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
      // The CLI does not have a --download-in-parallel flag.
      // We keep the frontend checkbox for UI/intent, but do not forward an unsupported flag.
    }
    if (this.skipExisting) {
      parts.push('--skip-existing');
    }
    const cmdStr = parts.join(' ');

    // Show command immediately in the status window
    this.status = 'Running: ' + cmdStr;
    this.appendLog(this.status);

    // Mark running state and call backend to run the CLI
    this.isRunning = true;
    RunCLIFetch(this.inputFilePath, this.outputDirPath, this.maxConnections, this.maxRetries, this.simultaneousDownloads, this.skipExisting, this.downloadInParallel)
      .then((result: string) => {
        this.status = result;
        this.appendLog(result);
        this.isRunning = false;
      })
      .catch(err => {
        this.status = "Error: " + err;
        this.appendLog(this.status);
        this.isRunning = false;
      });
  }

  // Request cancellation of the running fetch
  onCancelRun() {
    CancelRun()
      .then(() => {
        this.appendLog('[GUI] Cancel requested from frontend');
        this.isRunning = false;
      })
      .catch(err => {
        this.appendLog('[GUI] Cancel error: ' + err);
      });
  }

  // Set access token in the backend
  setAccessToken() {
    SetAccessToken(this.accessTokenInput)
      .then(() => {
        this.appendLog('[GUI] Access token set');
      })
      .catch((err: any) => this.appendLog('[GUI] Set token error: ' + err));
  }

  clearAccessToken() {
    ClearAccessToken()
      .then(() => this.appendLog('[GUI] Access token cleared'))
      .catch((err: any) => this.appendLog('[GUI] Clear token error: ' + err));
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
