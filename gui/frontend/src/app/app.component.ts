import { Component, OnInit } from '@angular/core';
import { FetchFiles, OpenInputFileDialog, OpenOutputDirectoryDialog, RunFetch } from '../../wailsjs/go/main/App';
import { EventsOn } from '../../wailsjs/runtime';


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

    this.updateOverallProgress();

    EventsOn('output', (data: string) => {
      this.appendLog(data);
    });

    EventsOn('progress', (data: any) => {
      this.status = `[${data.processed}/${data.total}] ${data.percentage.toFixed(1)}% | Downloaded: ${data.downloaded} | Skipped: ${data.skipped} | Failed: ${data.failed}${data.eta} | Current: ${data.currentId}`;
    });
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
      this.status = "Please select an input TCIA file, and an output directory.";
      return;
    }

    this.outputLogs = [];
    RunFetch(this.inputFilePath, this.outputDirPath, this.maxConnections, this.maxRetries, this.simultaneousDownloads, this.skipExisting, this.downloadInParallel);
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
