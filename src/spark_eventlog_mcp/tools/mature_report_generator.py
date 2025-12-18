"""
HTML 报告生成器
"""

import json
import os
import aiofiles
from datetime import datetime
from typing import Dict, Any, List
from pathlib import Path
from ..models.mature_models import MatureAnalysisResult

class HTMLReportGenerator:
    """HTML 可视化报告生成器"""

    def __init__(self):
        self.template = self._load_template()

    def _load_template(self) -> str:
        # <link rel="preconnect" href="https://fonts.googleapis.com">
        # <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        # <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=Orbitron:wght@400;500;700;900&display=swap" rel="stylesheet">
        """加载 HTML 模板"""
        return """
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Spark Event Log Analysis Report</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                :root {
                    --bg-primary: #0f1419;
                    --bg-secondary: #1f2937;
                    --bg-accent: #374151;
                    --accent-primary: #3b82f6;
                    --accent-secondary: #8b5cf6;
                    --accent-success: #10b981;
                    --accent-warning: #f59e0b;
                    --accent-danger: #ef4444;
                    --text-primary: #f9fafb;
                    --text-secondary: #d1d5db;
                    --text-muted: #9ca3af;
                    --border-color: #4b5563;
                    --grid-size: 24px;
                }

                * {
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }

                body {
                    font-family: 'JetBrains Mono', monospace;
                    background: var(--bg-primary);
                    color: var(--text-primary);
                    line-height: 1.6;
                    overflow-x: hidden;
                    position: relative;
                }

                /* 动态背景网格 */
                body::before {
                    content: '';
                    position: fixed;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 100%;
                    background-image:
                        linear-gradient(rgba(0,255,65,0.03) 1px, transparent 1px),
                        linear-gradient(90deg, rgba(0,255,65,0.03) 1px, transparent 1px);
                    background-size: var(--grid-size) var(--grid-size);
                    animation: gridPulse 4s ease-in-out infinite alternate;
                    pointer-events: none;
                    z-index: -1;
                }

                @keyframes gridPulse {
                    0% { opacity: 0.3; }
                    100% { opacity: 0.1; }
                }

                /* 移除了动态扫描线效果 */

                /* 容器布局 */
                .container {
                    max-width: 1400px;
                    margin: 0 auto;
                    padding: 40px 20px;
                    position: relative;
                }

                /* 标题区域 */
                .header {
                    text-align: center;
                    margin-bottom: 60px;
                    position: relative;
                }

                .title {
                    font-family: 'Orbitron', monospace;
                    font-size: 3.5rem;
                    font-weight: 900;
                    background: linear-gradient(45deg, var(--accent-primary), var(--accent-secondary));
                    -webkit-background-clip: text;
                    -webkit-text-fill-color: transparent;
                    background-clip: text;
                    margin-bottom: 10px;
                    text-transform: uppercase;
                    letter-spacing: 4px;
                    animation: titleGlow 2s ease-in-out infinite alternate;
                }

                @keyframes titleGlow {
                    0% { text-shadow: 0 0 20px rgba(59,130,246,0.3); }
                    100% { text-shadow: 0 0 40px rgba(59,130,246,0.6), 0 0 80px rgba(139,92,246,0.3); }
                }

                .subtitle {
                    font-size: 1.2rem;
                    color: var(--text-secondary);
                    font-weight: 300;
                    letter-spacing: 2px;
                }

                /* 精简面板系统 - 2x2专业紧凑布局 */
                .metrics-dashboard {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    grid-template-rows: auto auto;
                    gap: 20px;
                    margin-bottom: 40px;
                }

                .metric-panel {
                    background: linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-accent) 100%);
                    border: 1px solid var(--border-color);
                    border-radius: 16px;
                    padding: 18px;
                    position: relative;
                    overflow: hidden;
                    transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
                    backdrop-filter: blur(20px);
                    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
                }

                .metric-panel::before {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: 0;
                    right: 0;
                    height: 4px;
                    background: linear-gradient(90deg, var(--accent-primary), var(--accent-secondary), var(--accent-success));
                    transform: scaleX(0);
                    transform-origin: left;
                    transition: transform 0.6s ease;
                }

                .metric-panel:hover {
                    transform: translateY(-8px);
                    border-color: var(--accent-primary);
                    box-shadow: 0 20px 60px rgba(59, 130, 246, 0.2);
                }

                .metric-panel:hover::before {
                    transform: scaleX(1);
                }

                /* 特殊布局 */
                .panel-app-info {
                    grid-column: 1;
                    grid-row: 1;
                }

                .panel-execution {
                    grid-column: 2;
                    grid-row: 1;
                }

                .panel-resources {
                    grid-column: 1 / -1;
                    grid-row: 2;
                    display: grid;
                    grid-template-columns: 1fr 1fr 1fr;
                    gap: 30px;
                    padding: 40px;
                }

                .panel-shuffle {
                    grid-column: 1;
                    grid-row: 3;
                }

                .panel-driver {
                    grid-column: 2;
                    grid-row: 3;
                }

                .card {
                    background: var(--bg-secondary);
                    border: 1px solid var(--border-color);
                    border-radius: 12px;
                    padding: 30px;
                    position: relative;
                    overflow: hidden;
                    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                    backdrop-filter: blur(10px);
                }

                .card::before {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: 0;
                    right: 0;
                    height: 3px;
                    background: linear-gradient(90deg, var(--accent-green), var(--accent-blue), var(--accent-purple));
                    transform: scaleX(0);
                    transform-origin: left;
                    transition: transform 0.3s ease;
                }

                .card:hover {
                    transform: translateY(-5px);
                    border-color: var(--accent-green);
                    box-shadow: 0 20px 40px rgba(0,255,65,0.1);
                }

                .card:hover::before {
                    transform: scaleX(1);
                }

                .card-title {
                    font-family: 'Orbitron', monospace;
                    font-size: 1.4rem;
                    font-weight: 700;
                    color: var(--accent-primary);
                    margin-bottom: 20px;
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }

                .card-title::before {
                    content: '▶';
                    font-size: 0.8rem;
                    animation: blink 2s infinite;
                }

                @keyframes blink {
                    0%, 50% { opacity: 1; }
                    51%, 100% { opacity: 0.3; }
                }

                /* 新的紧凑面板样式 */
                .panel-header {
                    margin-bottom: 15px;
                    border-bottom: 1px solid rgba(255,255,255,0.1);
                    padding-bottom: 10px;
                }

                .panel-title {
                    font-family: 'Orbitron', monospace;
                    font-size: 1.2rem;
                    font-weight: 700;
                    color: var(--accent-primary);
                    margin: 0 0 3px 0;
                    line-height: 1.3;
                }

                .panel-subtitle {
                    font-size: 0.8rem;
                    color: var(--text-muted);
                    font-weight: 300;
                    line-height: 1.3;
                }

                /* 应用概览面板样式 */
                .metrics-grid {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 20px;
                }

                .metric-group {
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                }

                .metric-compact {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    padding: 6px 0;
                }

                .metric-icon {
                    font-size: 1rem;
                    opacity: 0.8;
                }

                .metric-content {
                    flex: 1;
                }

                .metric-label-compact {
                    display: block;
                    font-size: 0.75rem;
                    color: var(--text-muted);
                    margin-bottom: 1px;
                    line-height: 1.2;
                }

                .metric-value-compact {
                    display: block;
                    font-weight: 600;
                    font-size: 0.85rem;
                    color: var(--text-primary);
                    line-height: 1.2;
                }

                /* Executor 资源统计卡片 */
                .resource-stats {
                    display: grid;
                    grid-template-columns: repeat(2, 1fr);
                    gap: 15px;
                }

                .stat-card {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                    padding: 15px;
                    background: rgba(255, 255, 255, 0.02);
                    border: 1px solid rgba(255, 255, 255, 0.05);
                    border-radius: 8px;
                    transition: all 0.3s ease;
                }

                .stat-card:hover {
                    background: rgba(255, 255, 255, 0.05);
                    border-color: var(--accent-primary);
                }

                .stat-icon {
                    font-size: 1.5rem;
                    opacity: 0.8;
                }

                .stat-content {
                    flex: 1;
                }

                .stat-value {
                    font-weight: 700;
                    font-size: 1.1rem;
                    color: var(--text-primary);
                    margin-bottom: 2px;
                }

                .stat-label {
                    font-size: 0.8rem;
                    color: var(--text-muted);
                }

                /* Shuffle & Driver 联合面板 */
                .dual-section {
                    display: flex;
                    align-items: stretch;
                    gap: 20px;
                }

                .section-left,
                .section-right {
                    flex: 1;
                }

                .section-divider {
                    width: 1px;
                    background: linear-gradient(to bottom, transparent, var(--accent-primary), transparent);
                    opacity: 0.3;
                }

                .section-header {
                    margin-bottom: 15px;
                }

                .section-title {
                    font-family: 'Orbitron', monospace;
                    font-size: 1.1rem;
                    font-weight: 600;
                    color: var(--accent-secondary);
                    margin: 0;
                }

                .compact-metrics {
                    display: flex;
                    flex-direction: column;
                    gap: 10px;
                }

                .compact-metric {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 8px 0;
                    border-bottom: 1px solid rgba(255,255,255,0.05);
                }

                .compact-metric:last-child {
                    border-bottom: none;
                }

                .compact-label {
                    font-size: 0.9rem;
                    color: var(--text-secondary);
                }

                .compact-value {
                    font-weight: 600;
                    font-size: 0.95rem;
                    color: var(--text-primary);
                }

                /* 合并面板的新布局样式 */
                .combined-layout {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 25px;
                    align-items: start;
                }

                .app-info-section,
                .executor-section {
                    display: flex;
                    flex-direction: column;
                }

                .app-info-section .section-title,
                .executor-section .section-title {
                    font-family: 'Orbitron', monospace;
                    font-size: 1.1rem;
                    font-weight: 600;
                    color: var(--accent-secondary);
                    margin: 0 0 15px 0;
                    padding-bottom: 8px;
                    border-bottom: 1px solid rgba(255,255,255,0.1);
                }

                .cluster-summary {
                    font-size: 0.9rem;
                    color: var(--text-muted);
                    margin-bottom: 15px;
                    text-align: center;
                    padding: 8px 15px;
                    background: rgba(255,255,255,0.03);
                    border-radius: 8px;
                    border: 1px solid rgba(255,255,255,0.05);
                }

                /* 迷你资源统计卡片 */
                .resource-stats-mini {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 8px;
                }

                .stat-card-mini {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    padding: 10px;
                    background: rgba(255, 255, 255, 0.02);
                    border: 1px solid rgba(255, 255, 255, 0.05);
                    border-radius: 6px;
                    transition: all 0.3s ease;
                    min-height: 60px;
                }

                .stat-card-mini:hover {
                    background: rgba(255, 255, 255, 0.05);
                    border-color: var(--accent-primary);
                    transform: translateY(-2px);
                }

                .stat-card-mini .stat-icon {
                    font-size: 1.2rem;
                    opacity: 0.8;
                }

                .stat-card-mini .stat-content {
                    flex: 1;
                    min-width: 0;
                }

                .stat-card-mini .stat-value {
                    font-weight: 600;
                    font-size: 0.9rem;
                    color: var(--text-primary);
                    margin-bottom: 2px;
                    word-break: break-all;
                }

                .stat-card-mini .stat-label {
                    font-size: 0.7rem;
                    color: var(--text-muted);
                    line-height: 1.2;
                }

                /* 数据显示 */
                .metric {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 15px 0;
                    border-bottom: 1px solid rgba(255,255,255,0.1);
                }

                .metric:last-child {
                    border-bottom: none;
                }

                .metric-label {
                    font-size: 0.95rem;
                    color: var(--text-secondary);
                }

                .metric-value {
                    font-weight: 600;
                    font-size: 1.1rem;
                    color: var(--text-primary);
                }

                .metric-value.highlight {
                    color: var(--accent-green);
                    text-shadow: 0 0 10px rgba(0,255,65,0.3);
                }

                /* 状态指示器 */
                .status {
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;
                    padding: 6px 12px;
                    border-radius: 20px;
                    font-size: 0.85rem;
                    font-weight: 500;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                }

                .status.success {
                    background: rgba(0,255,65,0.1);
                    color: var(--accent-green);
                    border: 1px solid rgba(0,255,65,0.3);
                }

                .status.warning {
                    background: rgba(255,107,53,0.1);
                    color: var(--accent-orange);
                    border: 1px solid rgba(255,107,53,0.3);
                }

                .status.error {
                    background: rgba(255,69,58,0.1);
                    color: #ff453a;
                    border: 1px solid rgba(255,69,58,0.3);
                }

                .status::before {
                    content: '';
                    width: 6px;
                    height: 6px;
                    border-radius: 50%;
                    background: currentColor;
                    animation: statusPulse 2s infinite;
                }

                @keyframes statusPulse {
                    0%, 100% { opacity: 1; }
                    50% { opacity: 0.3; }
                }

                /* 图表容器 */
                .chart-container {
                    background: var(--bg-secondary);
                    border: 1px solid var(--border-color);
                    border-radius: 12px;
                    padding: 30px;
                    margin: 30px 0;
                    position: relative;
                }

                .chart-title {
                    font-family: 'Orbitron', monospace;
                    font-size: 1.4rem;
                    font-weight: 700;
                    background: linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-secondary) 50%, var(--accent-success) 100%);
                    -webkit-background-clip: text;
                    -webkit-text-fill-color: transparent;
                    background-clip: text;
                    margin-bottom: 25px;
                    text-align: center;
                    text-transform: uppercase;
                    letter-spacing: 2px;
                    position: relative;
                    padding-bottom: 15px;
                }

                .chart-title::after {
                    content: '';
                    position: absolute;
                    bottom: 0;
                    left: 50%;
                    transform: translateX(-50%);
                    width: 80px;
                    height: 3px;
                    background: linear-gradient(90deg, var(--accent-primary), var(--accent-secondary), var(--accent-success));
                    border-radius: 2px;
                    animation: titlePulse 2s ease-in-out infinite alternate;
                }

                @keyframes titlePulse {
                    0% { opacity: 0.6; transform: translateX(-50%) scaleX(1); }
                    100% { opacity: 1; transform: translateX(-50%) scaleX(1.2); }
                }

                /* 优化建议面板 - 美观单面板设计 */
                .recommendations-panel {
                    background: linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-accent) 100%);
                    border: 1px solid var(--border-color);
                    box-shadow: 0 20px 60px rgba(59, 130, 246, 0.15);
                }

                .recommendations-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                    gap: 20px;
                    margin-top: 10px;
                }

                .recommendation-item {
                    background: linear-gradient(135deg, rgba(59, 130, 246, 0.08) 0%, rgba(139, 92, 246, 0.08) 100%);
                    border: 1px solid rgba(59, 130, 246, 0.2);
                    border-radius: 16px;
                    padding: 25px;
                    position: relative;
                    overflow: hidden;
                    transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
                    backdrop-filter: blur(10px);
                    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.15);
                }

                .recommendation-item::before {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: 0;
                    right: 0;
                    height: 4px;
                    background: linear-gradient(90deg, var(--accent-primary), var(--accent-secondary), var(--accent-success));
                    transform: scaleX(0);
                    transform-origin: left;
                    transition: transform 0.6s ease;
                }

                .recommendation-item:hover {
                    transform: translateY(-8px) scale(1.02);
                    border-color: var(--accent-primary);
                    box-shadow: 0 25px 80px rgba(59, 130, 246, 0.25);
                    background: linear-gradient(135deg, rgba(59, 130, 246, 0.12) 0%, rgba(139, 92, 246, 0.12) 100%);
                }

                .recommendation-item:hover::before {
                    transform: scaleX(1);
                }

                .recommendation-priority {
                    position: absolute;
                    top: -12px;
                    right: 20px;
                    padding: 8px 16px;
                    border-radius: 20px;
                    font-size: 0.75rem;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
                    animation: priorityPulse 3s ease-in-out infinite alternate;
                }

                @keyframes priorityPulse {
                    0% { transform: scale(1); }
                    100% { transform: scale(1.05); }
                }

                .priority-high {
                    background: linear-gradient(135deg, #ff453a 0%, #ff6b35 100%);
                    color: white;
                    border: 1px solid rgba(255, 69, 58, 0.6);
                    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
                }

                .priority-medium {
                    background: linear-gradient(135deg, #f59e0b 0%, #fbbf24 100%);
                    color: white;
                    border: 1px solid rgba(245, 158, 11, 0.6);
                    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
                }

                .priority-low {
                    background: linear-gradient(135deg, #10b981 0%, #34d399 100%);
                    color: white;
                    border: 1px solid rgba(16, 185, 129, 0.6);
                    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
                }

                .recommendation-title {
                    font-family: 'Orbitron', monospace;
                    font-size: 1.3rem;
                    font-weight: 700;
                    background: linear-gradient(135deg, var(--accent-primary) 0%, var(--accent-secondary) 100%);
                    -webkit-background-clip: text;
                    -webkit-text-fill-color: transparent;
                    background-clip: text;
                    margin: 0 0 15px 0;
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }

                .recommendation-title::before {
                    content: '⚡';
                    font-size: 1.1rem;
                    animation: sparkle 2s ease-in-out infinite alternate;
                }

                @keyframes sparkle {
                    0% { opacity: 0.7; transform: rotate(0deg); }
                    100% { opacity: 1; transform: rotate(15deg); }
                }

                .recommendation-description {
                    color: var(--text-secondary);
                    font-size: 0.95rem;
                    line-height: 1.6;
                    margin-bottom: 18px;
                    padding-left: 15px;
                    border-left: 3px solid rgba(59, 130, 246, 0.3);
                }

                .recommendation-suggestion {
                    background: linear-gradient(135deg, rgba(16, 185, 129, 0.1) 0%, rgba(52, 211, 153, 0.1) 100%);
                    padding: 18px;
                    border-radius: 12px;
                    border: 1px solid rgba(16, 185, 129, 0.2);
                    position: relative;
                    overflow: hidden;
                }

                .recommendation-suggestion::before {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: 0;
                    width: 4px;
                    height: 100%;
                    background: linear-gradient(to bottom, var(--accent-success), var(--accent-secondary));
                    border-radius: 0 2px 2px 0;
                }

                .recommendation-suggestion-label {
                    color: var(--accent-success);
                    font-weight: 700;
                    font-size: 0.9rem;
                    margin-bottom: 8px;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                }

                .recommendation-suggestion-text {
                    color: var(--text-primary);
                    font-size: 0.95rem;
                    line-height: 1.5;
                    margin-left: 12px;
                }

                .recommendation-config {
                    margin-top: 15px;
                    padding: 12px 16px;
                    background: rgba(59, 130, 246, 0.08);
                    border: 1px solid rgba(59, 130, 246, 0.2);
                    border-radius: 8px;
                    font-family: 'JetBrains Mono', monospace;
                    font-size: 0.85rem;
                    color: var(--accent-primary);
                    position: relative;
                }

                .recommendation-config::before {
                    content: '⚙️';
                    margin-right: 8px;
                    font-size: 0.9rem;
                }

                .recommendation-config-label {
                    font-weight: 700;
                    color: var(--accent-secondary);
                    margin-right: 8px;
                }

                /* 响应式设计 */
                @media (max-width: 1200px) {
                    .recommendations-grid {
                        grid-template-columns: 1fr;
                        gap: 16px;
                    }
                }

                @media (max-width: 768px) {
                    .recommendation-item {
                        padding: 20px;
                        margin-bottom: 15px;
                    }

                    .recommendation-priority {
                        position: static;
                        margin-bottom: 15px;
                        display: inline-block;
                    }

                    .recommendation-title {
                        font-size: 1.1rem;
                    }
                }

                /* 响应式设计 */
                @media (max-width: 1200px) {
                    .metrics-dashboard {
                        grid-template-columns: 1fr;
                        grid-template-rows: auto auto auto;
                    }

                    .metrics-grid {
                        grid-template-columns: 1fr;
                        gap: 15px;
                    }

                    .resource-stats {
                        grid-template-columns: repeat(2, 1fr);
                        gap: 12px;
                    }

                    .dual-section {
                        flex-direction: column;
                        gap: 25px;
                    }

                    .section-divider {
                        display: none;
                    }
                }

                @media (max-width: 768px) {
                    .title {
                        font-size: 2.5rem;
                        letter-spacing: 2px;
                    }

                    .container {
                        padding: 20px 15px;
                    }

                    .metric-panel {
                        padding: 20px;
                    }

                    .resource-stats {
                        grid-template-columns: 1fr;
                        gap: 10px;
                    }

                    .panel-header {
                        margin-bottom: 15px;
                        padding-bottom: 10px;
                    }

                    .panel-title {
                        font-size: 1.2rem;
                    }

                    .stat-card {
                        padding: 12px;
                    }

                    .metrics-grid {
                        gap: 10px;
                        grid-template-columns: 1fr;
                    }

                    .metric {
                        flex-direction: column;
                        align-items: flex-start;
                        gap: 5px;
                    }

                    .metric-label {
                        font-size: 0.85rem;
                    }

                    .metric-value {
                        font-size: 1rem;
                    }

                    /* 图表容器移动端适配 */
                    .chart-container {
                        padding: 20px 10px;
                        margin: 20px 0;
                    }

                    .chart-title {
                        font-size: 1.1rem;
                        margin-bottom: 15px;
                    }
                }

                /* 手机端适配 */
                @media (max-width: 480px) {
                    .title {
                        font-size: 2rem;
                        letter-spacing: 1px;
                    }

                    .subtitle {
                        font-size: 1rem;
                        letter-spacing: 1px;
                    }

                    .container {
                        padding: 15px 10px;
                    }

                    .metric-panel {
                        padding: 15px 10px;
                        border-radius: 8px;
                    }

                    .card-title {
                        font-size: 1.2rem;
                        margin-bottom: 15px;
                    }

                    .panel-resources {
                        padding: 20px 10px;
                    }

                    .resource-section h4 {
                        font-size: 1rem;
                        margin-bottom: 10px;
                    }

                    /* 表格手机端适配 */
                    .data-table {
                        font-size: 0.75rem;
                    }

                    .data-table th,
                    .data-table td {
                        padding: 10px 8px;
                    }

                    .data-table .host-cell {
                        max-width: 120px;
                        font-size: 0.7rem;
                    }

                    /* 图表标题手机端适配 */
                    .chart-title {
                        font-size: 1rem;
                    }

                    /* 建议卡片手机端适配 */
                    .recommendation-item {
                        padding: 15px 10px;
                        margin-bottom: 15px;
                    }

                    .recommendation-priority {
                        position: static;
                        margin-bottom: 10px;
                        display: inline-block;
                    }
                }

                /* 加载动画 */
                .loading {
                    display: inline-block;
                    width: 20px;
                    height: 20px;
                    border: 3px solid rgba(0,255,65,0.3);
                    border-radius: 50%;
                    border-top-color: var(--accent-green);
                    animation: spin 1s ease-in-out infinite;
                }

                @keyframes spin {
                    to { transform: rotate(360deg); }
                }

                /* 精美数据表格 */
                .data-table {
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                    background: var(--bg-secondary);
                    border-radius: 12px;
                    overflow: hidden;
                    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
                    position: relative;
                }

                .data-table::before {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: 0;
                    right: 0;
                    height: 3px;
                    background: linear-gradient(90deg, var(--accent-primary), var(--accent-secondary), var(--accent-success));
                }

                .data-table th,
                .data-table td {
                    padding: 18px 20px;
                    text-align: left;
                    border-bottom: 1px solid rgba(255,255,255,0.08);
                    position: relative;
                }

                .data-table th {
                    background: linear-gradient(135deg, var(--bg-accent) 0%, rgba(75, 85, 99, 0.8) 100%);
                    font-weight: 700;
                    color: var(--accent-primary);
                    font-family: 'Orbitron', monospace;
                    font-size: 0.95rem;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    position: sticky;
                    top: 0;
                    z-index: 10;
                }

                .data-table th::after {
                    content: '';
                    position: absolute;
                    bottom: 0;
                    left: 0;
                    right: 0;
                    height: 2px;
                    background: linear-gradient(90deg, transparent, var(--accent-primary), transparent);
                }

                .data-table td {
                    font-family: 'JetBrains Mono', monospace;
                    font-size: 0.9rem;
                    transition: all 0.3s ease;
                }

                .data-table tbody tr {
                    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                }

                .data-table tbody tr:hover {
                    background: linear-gradient(90deg, rgba(59, 130, 246, 0.1), rgba(139, 92, 246, 0.1));
                    transform: translateX(5px);
                    box-shadow: inset 4px 0 0 var(--accent-primary);
                }

                .data-table tbody tr:hover td {
                    color: var(--accent-primary);
                }

                /* 表格数据类型高亮 */
                .data-table .memory-cell {
                    color: var(--accent-success);
                    font-weight: 600;
                }

                .data-table .id-cell {
                    color: var(--accent-secondary);
                    font-weight: 700;
                }

                .data-table .host-cell {
                    color: var(--text-secondary);
                    font-size: 0.8rem;
                    max-width: 200px;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }

                .data-table .cores-cell {
                    color: var(--accent-warning);
                    font-weight: 600;
                    text-align: center;
                }

                /* 表格排序功能样式 */
                .sortable {
                    cursor: pointer;
                    user-select: none;
                    position: relative;
                    transition: all 0.3s ease;
                }

                .sortable:hover {
                    background: linear-gradient(135deg, var(--bg-accent) 0%, rgba(59, 130, 246, 0.1) 100%);
                    transform: translateY(-2px);
                }

                .sort-indicator {
                    margin-left: 8px;
                    font-size: 0.9rem;
                    opacity: 0.6;
                    transition: all 0.3s ease;
                }

                .sortable:hover .sort-indicator {
                    opacity: 1;
                    color: var(--accent-primary);
                }

                .sortable.sort-asc .sort-indicator::before {
                    content: '↑';
                    color: var(--accent-success);
                    font-weight: bold;
                }

                .sortable.sort-desc .sort-indicator::before {
                    content: '↓';
                    color: var(--accent-danger);
                    font-weight: bold;
                }

                .sortable.sort-asc .sort-indicator,
                .sortable.sort-desc .sort-indicator {
                    opacity: 1;
                }

                /* 进度条 */
                .progress-bar {
                    width: 100%;
                    height: 8px;
                    background: var(--bg-accent);
                    border-radius: 4px;
                    overflow: hidden;
                    margin: 10px 0;
                }

                .progress-fill {
                    height: 100%;
                    background: linear-gradient(90deg, var(--accent-green), var(--accent-blue));
                    border-radius: 4px;
                    transition: width 1s ease;
                    position: relative;
                }

                .progress-fill::after {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: 0;
                    right: 0;
                    bottom: 0;
                    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
                    animation: shimmer 2s infinite;
                }

                @keyframes shimmer {
                    0% { transform: translateX(-100%); }
                    100% { transform: translateX(100%); }
                }
            </style>
        </head>
        <body>
            <div class="scan-line"></div>

            <div class="container">
                <header class="header">
                    <h1 class="title">Spark Analytics</h1>
                    <p class="subtitle">Event Log Analysis Report</p>
                </header>

                <!-- 2x2 紧凑指标仪表板 -->
                <div class="metrics-dashboard">
                    <!-- 应用 & 集群统览面板 - 合并应用信息和Executor资源 -->
                    <div class="metric-panel panel-combined">
                        <div class="panel-header">
                            <h3 class="panel-title">🚀 应用 & 集群统览</h3>
                            <div class="panel-subtitle">{{application_name}} • {{spark_version}} • {{total_executors}} 个executor节点</div>
                        </div>

                        <div class="combined-layout">
                            <!-- 左侧：应用基础信息 -->
                            <div class="app-info-section">
                                <h4 class="section-title">📊 应用信息</h4>
                                <div class="metric-group">
                                    <div class="metric-compact">
                                        <span class="metric-icon">🆔</span>
                                        <div class="metric-content">
                                            <span class="metric-label-compact">Application ID</span>
                                            <span class="metric-value-compact">{{application_id}}</span>
                                        </div>
                                    </div>
                                    <div class="metric-compact">
                                        <span class="metric-icon">⏱️</span>
                                        <div class="metric-content">
                                            <span class="metric-label-compact">Duration</span>
                                            <span class="metric-value-compact highlight">{{duration_formatted}}</span>
                                        </div>
                                    </div>
                                    <div class="metric-compact">
                                        <span class="metric-icon">📋</span>
                                        <div class="metric-content">
                                            <span class="metric-label-compact">Total Jobs</span>
                                            <span class="metric-value-compact">{{total_jobs}}</span>
                                        </div>
                                    </div>
                                    <div class="metric-compact">
                                        <span class="metric-icon">✅</span>
                                        <div class="metric-content">
                                            <span class="metric-label-compact">Success Rate</span>
                                            <span class="metric-value-compact highlight">{{success_rate}}%</span>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <!-- 右侧：Executor 集群资源 -->
                            <div class="executor-section">
                                <h4 class="section-title">💻 Executor 资源</h4>
                                <div class="cluster-summary">{{executor_cores_config}} 核 • {{executor_memory_config}}</div>
                                <div class="resource-stats-mini">
                                    <div class="stat-card-mini">
                                        <span class="stat-icon">💾</span>
                                        <div class="stat-content">
                                            <div class="stat-value highlight">{{executor_total_memory}}</div>
                                            <div class="stat-label">Total Memory</div>
                                        </div>
                                    </div>
                                    <div class="stat-card-mini">
                                        <span class="stat-icon">🗄️</span>
                                        <div class="stat-content">
                                            <div class="stat-value highlight">{{executor_overhead_memory}}</div>
                                            <div class="stat-label">Overhead Memory</div>
                                        </div>
                                    </div>
                                    <div class="stat-card-mini">
                                        <span class="stat-icon">⚡</span>
                                        <div class="stat-content">
                                            <div class="stat-value">{{peak_memory_formatted}}</div>
                                            <div class="stat-label">Peak Memory</div>
                                        </div>
                                    </div>
                                    <div class="stat-card-mini">
                                        <span class="stat-icon">🔄</span>
                                        <div class="stat-content">
                                            <div class="stat-value">{{avg_executor_overhead_memory}}</div>
                                            <div class="stat-label">Overhead/Exec</div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <!-- Shuffle & Driver 联合面板 -->
                    <div class="metric-panel panel-shuffle-driver">
                        <div class="dual-section">
                            <!-- Shuffle 部分 -->
                            <div class="section-left">
                                <div class="section-header">
                                    <h4 class="section-title">🔄 Shuffle 分析</h4>
                                </div>
                                <div class="compact-metrics">
                                    <div class="compact-metric">
                                        <span class="compact-label">Read</span>
                                        <span class="compact-value highlight">{{shuffle_read_formatted}}</span>
                                    </div>
                                    <div class="compact-metric">
                                        <span class="compact-label">Write</span>
                                        <span class="compact-value highlight">{{shuffle_write_formatted}}</span>
                                    </div>
                                    <div class="compact-metric">
                                        <span class="compact-label">Records</span>
                                        <span class="compact-value">{{shuffle_records_formatted}}</span>
                                    </div>
                                    <div class="compact-metric">
                                        <span class="compact-label">Efficiency</span>
                                        <span class="compact-value">{{shuffle_efficiency}}</span>
                                    </div>
                                </div>
                            </div>

                            <!-- 分隔线 -->
                            <div class="section-divider"></div>

                            <!-- Driver 部分 -->
                            <div class="section-right">
                                <div class="section-header">
                                    <h4 class="section-title">🎛️ Driver 资源</h4>
                                </div>
                                <div class="compact-metrics">
                                    <div class="compact-metric">
                                        <span class="compact-label">Cores</span>
                                        <span class="compact-value">{{driver_cores}}</span>
                                    </div>
                                    <div class="compact-metric">
                                        <span class="compact-label">Memory</span>
                                        <span class="compact-value highlight">{{driver_memory}}</span>
                                    </div>
                                    <div class="compact-metric">
                                        <span class="compact-label">Overhead Memory</span>
                                        <span class="compact-value highlight">{{driver_overhead_memory_formatted}}</span>
                                    </div>
                                    <div class="compact-metric">
                                        <span class="compact-label">GC Time</span>
                                        <span class="compact-value">{{driver_gc_time_formatted}}</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Shuffle Stage 分析图表 -->
                <div class="chart-container">
                    <h3 class="chart-title">🔄 Shuffle 密集型 Stage 分析</h3>
                    <div id="shuffleStagesChart"></div>
                </div>

                <!-- Executor 资源使用图表 -->
                <div class="chart-container">
                    <h3 class="chart-title">💻 Executor 资源使用分布</h3>
                    <div id="executorResourceChart"></div>
                </div>

                <!-- Executor 按 Stage 的 Shuffle 使用分布 -->
                <div class="chart-container">
                    <h3 class="chart-title">🎯 Stage-Executor Shuffle 分布</h3>
                    <div id="stageExecutorShuffleChart"></div>
                </div>

                <!-- 数据倾斜分析 -->
                <div class="chart-container">
                    <h3 class="chart-title">⚖️ 数据倾斜检测</h3>
                    <div id="dataSkewChart"></div>
                </div>

                <!-- 优化建议 -->
                <div class="chart-container recommendations-panel">
                    <h3 class="chart-title">💡 智能优化建议</h3>
                    <div class="recommendations-grid">
                        {{recommendations_html}}
                    </div>
                </div>

                <!-- 详细指标表格 -->
                <div class="chart-container">
                    <h3 class="chart-title">📊 详细Executor Shuffle信息</h3>
                    {{metrics_table}}
                </div>
            </div>

            <script>
                // Shuffle Stage 分析图表 - 读写分离
                function createShuffleStagesChart(data) {
                    const trace1 = {
                        x: data.stage_names,
                        y: data.shuffle_read_bytes,
                        name: 'Shuffle Read',
                        type: 'bar',
                        marker: {
                            color: '#3b82f6',
                            line: {
                                color: '#1e40af',
                                width: 1
                            }
                        },
                        text: data.shuffle_read_bytes.map(bytes => formatBytes(bytes)),
                        textposition: 'auto',
                        hovertemplate: '<b>%{x}</b><br>Shuffle Read: %{text}<extra></extra>'
                    };

                    const trace2 = {
                        x: data.stage_names,
                        y: data.shuffle_write_bytes,
                        name: 'Shuffle Write',
                        type: 'bar',
                        marker: {
                            color: '#8b5cf6',
                            line: {
                                color: '#6d28d9',
                                width: 1
                            }
                        },
                        text: data.shuffle_write_bytes.map(bytes => formatBytes(bytes)),
                        textposition: 'auto',
                        hovertemplate: '<b>%{x}</b><br>Shuffle Write: %{text}<extra></extra>'
                    };

                    const layout = {
                        paper_bgcolor: 'rgba(0,0,0,0)',
                        plot_bgcolor: 'rgba(0,0,0,0)',
                        font: { color: '#ffffff', family: 'JetBrains Mono' },
                        xaxis: {
                            gridcolor: '#333333',
                            tickfont: { size: 10 }
                        },
                        yaxis: {
                            gridcolor: '#333333',
                            title: 'Shuffle Bytes'
                        },
                        barmode: 'group',
                        margin: { t: 20, b: 50, l: 80, r: 20 }
                    };

                    Plotly.newPlot('shuffleStagesChart', [trace1, trace2], layout, {
                        displayModeBar: false,
                        responsive: true
                    });

                    // 图例交互：双击单选，单击切换
                    let shuffleStagesLastClick = 0;
                    document.getElementById('shuffleStagesChart').on('plotly_legendclick', function(data) {
                        const chartDiv = document.getElementById('shuffleStagesChart');
                        const currentTime = new Date().getTime();
                        const timeDiff = currentTime - shuffleStagesLastClick;

                        if (timeDiff < 400) {
                            // 双击：单选模式，只显示点击的序列
                            const update = {};
                            chartDiv.data.forEach((trace, index) => {
                                update[`visible[${index}]`] = index === data.curveNumber ? true : false;
                            });
                            Plotly.restyle('shuffleStagesChart', update);
                            shuffleStagesLastClick = 0; // 重置点击时间
                            return false;
                        } else {
                            // 单击：正常切换显示/隐藏
                            shuffleStagesLastClick = currentTime;
                            return true; // 允许默认行为
                        }
                    });
                }

                // Executor 资源使用图表 - 内存和 Shuffle 分析
                function createExecutorResourceChart(data) {
                    const trace1 = {
                        x: data.executor_ids,
                        y: data.configured_memory,
                        name: 'Configured Memory',
                        type: 'bar',
                        marker: { color: '#10b981' },
                        text: data.configured_memory.map(bytes => formatBytes(bytes)),
                        textposition: 'auto',
                        hovertemplate: '<b>Executor %{x}</b><br>Configured Memory: %{text}<extra></extra>'
                    };

                    const trace2 = {
                        x: data.executor_ids,
                        y: data.actual_memory_used,
                        name: 'Actual Memory Used',
                        type: 'bar',
                        marker: { color: '#f59e0b' },
                        text: data.actual_memory_used.map(bytes => formatBytes(bytes)),
                        textposition: 'auto',
                        hovertemplate: '<b>Executor %{x}</b><br>Actual Memory: %{text}<extra></extra>'
                    };

                    const trace3 = {
                        x: data.executor_ids,
                        y: data.shuffle_read,
                        name: 'Shuffle Read',
                        type: 'bar',
                        marker: { color: '#3b82f6' },
                        text: data.shuffle_read.map(bytes => formatBytes(bytes)),
                        textposition: 'auto',
                        hovertemplate: '<b>Executor %{x}</b><br>Shuffle Read: %{text}<extra></extra>'
                    };

                    const trace4 = {
                        x: data.executor_ids,
                        y: data.shuffle_write,
                        name: 'Shuffle Write',
                        type: 'bar',
                        marker: { color: '#8b5cf6' },
                        text: data.shuffle_write.map(bytes => formatBytes(bytes)),
                        textposition: 'auto',
                        hovertemplate: '<b>Executor %{x}</b><br>Shuffle Write: %{text}<extra></extra>'
                    };

                    const layout = {
                        paper_bgcolor: 'rgba(0,0,0,0)',
                        plot_bgcolor: 'rgba(0,0,0,0)',
                        font: { color: '#ffffff', family: 'JetBrains Mono' },
                        xaxis: {
                            gridcolor: '#333333',
                            title: 'Executor ID'
                        },
                        yaxis: {
                            gridcolor: '#333333',
                            title: 'Bytes'
                        },
                        barmode: 'group',
                        margin: { t: 20, b: 50, l: 80, r: 20 }
                    };

                    Plotly.newPlot('executorResourceChart', [trace1, trace2, trace3, trace4], layout, {
                        displayModeBar: false,
                        responsive: true
                    });

                    // 图例交互：双击单选，单击切换
                    let executorResourceLastClick = 0;
                    document.getElementById('executorResourceChart').on('plotly_legendclick', function(data) {
                        const chartDiv = document.getElementById('executorResourceChart');
                        const currentTime = new Date().getTime();
                        const timeDiff = currentTime - executorResourceLastClick;

                        if (timeDiff < 400) {
                            // 双击：单选模式，只显示点击的序列
                            const update = {};
                            chartDiv.data.forEach((trace, index) => {
                                update[`visible[${index}]`] = index === data.curveNumber ? true : false;
                            });
                            Plotly.restyle('executorResourceChart', update);
                            executorResourceLastClick = 0;
                            return false;
                        } else {
                            // 单击：正常切换显示/隐藏
                            executorResourceLastClick = currentTime;
                            return true;
                        }
                    });
                }

                // Stage-Executor Shuffle 分布图表
                function createStageExecutorShuffleChart(data) {
                    if (!data || Object.keys(data).length === 0) {
                        return;
                    }

                    const traces = [];
                    const colors = ['#3b82f6', '#8b5cf6', '#10b981', '#f59e0b', '#ef4444'];

                    Object.keys(data).forEach((stageKey, index) => {
                        const stageData = data[stageKey];
                        const colorIndex = index % colors.length;

                        // Shuffle Read trace
                        traces.push({
                            x: stageData.executor_ids,
                            y: stageData.shuffle_read,
                            name: `Stage ${stageData.stage_id} - Read`,
                            type: 'bar',
                            marker: {
                                color: colors[colorIndex],
                                opacity: 0.8
                            },
                            text: stageData.shuffle_read.map(bytes => formatBytes(bytes)),
                            textposition: 'auto',
                            hovertemplate: `<b>Stage ${stageData.stage_id}</b><br>${stageData.stage_name}<br>Executor: %{x}<br>Shuffle Read: %{text}<extra></extra>`
                        });

                        // Shuffle Write trace
                        traces.push({
                            x: stageData.executor_ids,
                            y: stageData.shuffle_write,
                            name: `Stage ${stageData.stage_id} - Write`,
                            type: 'bar',
                            marker: {
                                color: colors[colorIndex],
                                opacity: 0.5
                            },
                            text: stageData.shuffle_write.map(bytes => formatBytes(bytes)),
                            textposition: 'auto',
                            hovertemplate: `<b>Stage ${stageData.stage_id}</b><br>${stageData.stage_name}<br>Executor: %{x}<br>Shuffle Write: %{text}<extra></extra>`
                        });
                    });

                    const layout = {
                        paper_bgcolor: 'rgba(0,0,0,0)',
                        plot_bgcolor: 'rgba(0,0,0,0)',
                        font: { color: '#ffffff', family: 'JetBrains Mono' },
                        xaxis: {
                            gridcolor: '#333333',
                            title: 'Executor ID'
                        },
                        yaxis: {
                            gridcolor: '#333333',
                            title: 'Shuffle Bytes'
                        },
                        barmode: 'group',
                        margin: { t: 20, b: 50, l: 80, r: 20 },
                        legend: {
                            orientation: 'h',
                            y: -0.2
                        }
                    };

                    Plotly.newPlot('stageExecutorShuffleChart', traces, layout, {
                        displayModeBar: false,
                        responsive: true
                    });

                    // 图例交互：双击单选，单击切换
                    let stageExecutorLastClick = 0;
                    document.getElementById('stageExecutorShuffleChart').on('plotly_legendclick', function(data) {
                        const chartDiv = document.getElementById('stageExecutorShuffleChart');
                        const currentTime = new Date().getTime();
                        const timeDiff = currentTime - stageExecutorLastClick;

                        if (timeDiff < 400) {
                            // 双击：单选模式，只显示点击的序列
                            const update = {};
                            chartDiv.data.forEach((trace, index) => {
                                update[`visible[${index}]`] = index === data.curveNumber ? true : false;
                            });
                            Plotly.restyle('stageExecutorShuffleChart', update);
                            stageExecutorLastClick = 0;
                            return false;
                        } else {
                            // 单击：正常切换显示/隐藏
                            stageExecutorLastClick = currentTime;
                            return true;
                        }
                    });
                }

                // 数据倾斜分析图表
                function createDataSkewChart(data) {
                    const trace = {
                        x: data.executor_ids,
                        y: data.skew_ratios,
                        type: 'scatter',
                        mode: 'markers+lines',
                        marker: {
                            size: 12,
                            color: data.skew_ratios,
                            colorscale: [[0, '#00ff41'], [0.5, '#ff6b35'], [1, '#ff453a']],
                            line: { color: '#ffffff', width: 2 }
                        },
                        line: { color: '#00ff41' },
                        hovertemplate: '<b>Executor %{x}</b><br>Skew Ratio: %{y:.2f}<extra></extra>'
                    };

                    const layout = {
                        paper_bgcolor: 'rgba(0,0,0,0)',
                        plot_bgcolor: 'rgba(0,0,0,0)',
                        font: { color: '#ffffff', family: 'JetBrains Mono' },
                        xaxis: {
                            gridcolor: '#333333',
                            title: 'Executor ID'
                        },
                        yaxis: {
                            gridcolor: '#333333',
                            title: 'Skew Ratio'
                        },
                        shapes: [{
                            type: 'line',
                            x0: 0,
                            x1: 1,
                            xref: 'paper',
                            y0: 3,
                            y1: 3,
                            line: { color: '#ff6b35', width: 2, dash: 'dash' }
                        }],
                        annotations: [{
                            x: 0.02,
                            y: 3.2,
                            xref: 'paper',
                            text: 'Skew Threshold',
                            showarrow: false,
                            font: { color: '#ff6b35', size: 10 }
                        }],
                        margin: { t: 20, b: 50, l: 80, r: 20 }
                    };

                    Plotly.newPlot('dataSkewChart', [trace], layout, {
                        displayModeBar: false,
                        responsive: true
                    });
                }

                // 格式化字节数
                function formatBytes(bytes) {
                    const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
                    if (bytes === 0 || bytes === null || bytes === undefined) return '0 B';

                    // 确保bytes是正数
                    const absBytes = Math.abs(Number(bytes));
                    if (!isFinite(absBytes)) return '0 B';

                    let i = Math.floor(Math.log(absBytes) / Math.log(1024));

                    // 确保i在合理范围内
                    if (isNaN(i) || i < 0) i = 0;
                    if (i >= sizes.length) i = sizes.length - 1;

                    const size = absBytes / Math.pow(1024, i);
                    return Math.round(size * 100) / 100 + ' ' + sizes[i];
                }

                // 初始化图表数据
                const chartData = {{chart_data}};

                // 创建所有图表
                if (chartData.shuffle_stages) {
                    createShuffleStagesChart(chartData.shuffle_stages);
                }

                if (chartData.executor_resources) {
                    createExecutorResourceChart(chartData.executor_resources);
                }

                if (chartData.stage_executor_shuffle) {
                    createStageExecutorShuffleChart(chartData.stage_executor_shuffle);
                }

                if (chartData.data_skew) {
                    createDataSkewChart(chartData.data_skew);
                }

                // 表格排序功能
                let sortDirection = {};

                function sortTable(columnIndex) {
                    const table = document.querySelector('.sortable-table');
                    const tbody = table.querySelector('tbody');
                    const rows = Array.from(tbody.querySelectorAll('tr'));
                    const headers = table.querySelectorAll('th.sortable');

                    // 切换排序方向
                    const currentDirection = sortDirection[columnIndex] || 'none';
                    let newDirection;
                    if (currentDirection === 'none' || currentDirection === 'desc') {
                        newDirection = 'asc';
                    } else {
                        newDirection = 'desc';
                    }
                    sortDirection[columnIndex] = newDirection;

                    // 清除所有列的排序样式
                    headers.forEach(header => {
                        header.classList.remove('sort-asc', 'sort-desc');
                    });

                    // 添加当前列的排序样式
                    headers[columnIndex].classList.add(`sort-${newDirection}`);

                    // 排序数据
                    rows.sort((a, b) => {
                        const aValue = a.cells[columnIndex].textContent.trim();
                        const bValue = b.cells[columnIndex].textContent.trim();

                        // 处理不同数据类型
                        let comparison = 0;

                        // 数字列（Executor ID, Cores, 内存大小, GC Time）
                        if (columnIndex === 0 || columnIndex === 2 || columnIndex === 3 || columnIndex === 4 || columnIndex === 5 || columnIndex === 6) {
                            // 提取数字部分进行比较
                            const aNum = parseFloat(aValue.replace(/[^0-9.]/g, '')) || 0;
                            const bNum = parseFloat(bValue.replace(/[^0-9.]/g, '')) || 0;
                            comparison = aNum - bNum;
                        } else {
                            // 字符串列（Host）
                            comparison = aValue.localeCompare(bValue);
                        }

                        return newDirection === 'asc' ? comparison : -comparison;
                    });

                    // 重新插入排序后的行
                    rows.forEach(row => tbody.appendChild(row));

                    // 添加排序动画
                    rows.forEach((row, index) => {
                        row.style.animation = `tableRowSlide 0.3s ease ${index * 0.02}s both`;
                    });
                }

                // 表格行动画
                const style = document.createElement('style');
                style.textContent = `
                    @keyframes tableRowSlide {
                        from {
                            opacity: 0.7;
                            transform: translateX(-10px);
                        }
                        to {
                            opacity: 1;
                            transform: translateX(0);
                        }
                    }
                `;
                document.head.appendChild(style);

                // 添加页面加载动画
                document.addEventListener('DOMContentLoaded', function() {
                    const cards = document.querySelectorAll('.card');
                    cards.forEach((card, index) => {
                        card.style.opacity = '0';
                        card.style.transform = 'translateY(50px)';
                        setTimeout(() => {
                            card.style.transition = 'opacity 0.6s ease, transform 0.6s ease';
                            card.style.opacity = '1';
                            card.style.transform = 'translateY(0)';
                        }, index * 100);
                    });
                });
            </script>
        </body>
        </html>
        """

    async def generate_html_report(self, result: MatureAnalysisResult, html_report_host_address="http://localhost:7799", transport_mode="streamable-http") -> str:
        """
        生成 HTML 可视化报告并保存到文件

        Args:
            result: 分析结果
            server_host: 服务器地址
            server_port: 服务器端口

        Returns:
            str: FastAPI 访问地址 (http://host:port/api/reports/filename.html)
        """
        # 格式化数据
        formatted_data = self._format_data(result)

        # 生成图表数据
        chart_data = self._generate_chart_data(result)

        # 生成建议 HTML
        recommendations_html = self._generate_recommendations_html(result.optimization_recommendations)

        # 生成指标表格
        metrics_table = self._generate_metrics_table(result)

        # 替换模板变量
        html_content = self.template

        # 基础信息替换
        html_content = html_content.replace('{{application_id}}', result.application_id)
        html_content = html_content.replace('{{application_name}}', result.application_name)
        html_content = html_content.replace('{{spark_version}}', result.spark_version)
        html_content = html_content.replace('{{duration_formatted}}', formatted_data['duration'])
        html_content = html_content.replace('{{total_jobs}}', str(result.total_jobs))
        html_content = html_content.replace('{{successful_jobs}}', str(result.successful_jobs))
        html_content = html_content.replace('{{failed_jobs}}', str(result.failed_jobs))
        html_content = html_content.replace('{{success_rate}}', formatted_data['success_rate'])
        html_content = html_content.replace('{{total_executors}}', str(result.total_executors))

        # 性能指标替换
        html_content = html_content.replace('{{peak_memory_formatted}}', formatted_data['peak_memory'])
        html_content = html_content.replace('{{cpu_time_formatted}}', formatted_data['cpu_time'])

        # Shuffle 指标替换
        html_content = html_content.replace('{{shuffle_read_formatted}}', formatted_data['shuffle_read'])
        html_content = html_content.replace('{{shuffle_write_formatted}}', formatted_data['shuffle_write'])
        html_content = html_content.replace('{{shuffle_records_formatted}}', formatted_data['shuffle_records'])
        html_content = html_content.replace('{{shuffle_efficiency}}', formatted_data['shuffle_efficiency'])

        # Executor 配置信息
        executor_cores_config = result.spark_properties.get('spark.executor.cores', '2')
        executor_memory_config = result.spark_properties.get('spark.executor.memory', '1g')
        html_content = html_content.replace('{{executor_cores_config}}', executor_cores_config)
        html_content = html_content.replace('{{executor_memory_config}}', executor_memory_config)

        # Executor 内存分析
        if result.executors:
            # Total Memory = sum of configured executor memory only (不包含driver和overhead)
            total_executor_memory = sum(exec.configured_memory_bytes for exec in result.executors)

            # Executor Overhead Memory = sum of executor overhead only (不包含driver overhead)
            total_executor_overhead = sum(exec.overhead_memory for exec in result.executors)

            # Single Executor Overhead Memory (固定值，不是平均值)
            single_executor_overhead = result.executors[0].overhead_memory if result.executors else 0

            html_content = html_content.replace('{{executor_configured_memory_total}}', self._format_bytes(total_executor_memory))
            html_content = html_content.replace('{{executor_total_memory}}', self._format_bytes(total_executor_memory))
            html_content = html_content.replace('{{executor_overhead_memory}}', self._format_bytes(total_executor_overhead))
            html_content = html_content.replace('{{avg_executor_overhead_memory}}', self._format_bytes(single_executor_overhead))
        else:
            html_content = html_content.replace('{{executor_configured_memory_total}}', 'N/A')
            html_content = html_content.replace('{{executor_total_memory}}', 'N/A')
            html_content = html_content.replace('{{executor_overhead_memory}}', 'N/A')
            html_content = html_content.replace('{{avg_executor_overhead_memory}}', 'N/A')

        # Driver 指标替换
        if result.driver_metrics:
            html_content = html_content.replace('{{driver_cores}}', str(result.driver_metrics.cores))
            html_content = html_content.replace('{{driver_memory}}', result.driver_metrics.memory)
            html_content = html_content.replace('{{driver_overhead_memory_formatted}}', self._format_bytes(result.driver_metrics.overhead_memory))
            html_content = html_content.replace('{{driver_gc_time_formatted}}', f"{result.driver_metrics.total_gc_time/1000:.1f}s")
        else:
            html_content = html_content.replace('{{driver_cores}}', 'N/A')
            html_content = html_content.replace('{{driver_memory}}', 'N/A')
            html_content = html_content.replace('{{driver_overhead_memory_formatted}}', 'N/A')
            html_content = html_content.replace('{{driver_gc_time_formatted}}', 'N/A')

        # 其他内容替换
        html_content = html_content.replace('{{recommendations_html}}', recommendations_html)
        html_content = html_content.replace('{{metrics_table}}', metrics_table)
        html_content = html_content.replace('{{chart_data}}', json.dumps(chart_data))

        # 生成文件名（使用应用ID和时间戳）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"spark_report_{result.application_id}_{timestamp}.html"

        # 确保 report_data 目录存在
        report_dir = Path("report_data")
        report_dir.mkdir(exist_ok=True)

        # 文件路径
        file_path = report_dir / filename
        # 绝对路径
        absolute_path = file_path.resolve()
        # 异步写入文件
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            await f.write(html_content)
        
        if  transport_mode=="streamable-http":
            # 返回 resource URL
            return f"{html_report_host_address}/api/reports/{filename}"
        else:
            return str(absolute_path)

    def _format_data(self, result: MatureAnalysisResult) -> Dict[str, str]:
        """格式化数据用于显示"""
        formatted = {}

        # 格式化持续时间
        if result.duration_ms:
            duration_sec = result.duration_ms / 1000
            if duration_sec > 3600:
                formatted['duration'] = f"{duration_sec/3600:.1f} hours"
            elif duration_sec > 60:
                formatted['duration'] = f"{duration_sec/60:.1f} minutes"
            else:
                formatted['duration'] = f"{duration_sec:.1f} seconds"
        else:
            formatted['duration'] = "N/A"

        # 成功率
        if result.total_jobs > 0:
            success_rate = (result.successful_jobs / result.total_jobs) * 100
            formatted['success_rate'] = f"{success_rate:.1f}"
        else:
            formatted['success_rate'] = "0"

        # 格式化内存
        formatted['peak_memory'] = self._format_bytes(result.performance_metrics.peak_execution_memory)

        # 格式化时间
        formatted['cpu_time'] = f"{result.performance_metrics.total_cpu_time_ms/1000:.1f}s"

        # 格式化 Shuffle
        formatted['shuffle_read'] = self._format_bytes(result.shuffle_analysis.total_shuffle_read_bytes)
        formatted['shuffle_write'] = self._format_bytes(result.shuffle_analysis.total_shuffle_write_bytes)

        # 格式化记录数
        total_records = result.shuffle_analysis.total_shuffle_read_records + result.shuffle_analysis.total_shuffle_write_records
        formatted['shuffle_records'] = f"{total_records:,}"

        # Shuffle 效率
        if result.shuffle_analysis.total_shuffle_write_bytes > 0:
            ratio = result.shuffle_analysis.total_shuffle_read_bytes / result.shuffle_analysis.total_shuffle_write_bytes
            formatted['shuffle_efficiency'] = f"{ratio:.2f}x"
        else:
            formatted['shuffle_efficiency'] = "N/A"

        return formatted

    def _format_bytes(self, bytes_size: int) -> str:
        """格式化字节数"""
        if bytes_size == 0:
            return "0 B"

        units = ['B', 'KB', 'MB', 'GB', 'TB']
        size = float(bytes_size)

        for unit in units:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0

        return f"{size:.1f} PB"

    def _generate_chart_data(self, result: MatureAnalysisResult) -> Dict[str, Any]:
        """生成图表数据"""
        chart_data = {}

        # Shuffle Stage 数据 - 分离读写
        if result.shuffle_analysis.most_shuffle_intensive_stages:
            stages = result.shuffle_analysis.most_shuffle_intensive_stages[:10]  # 取前10个
            chart_data['shuffle_stages'] = {
                'stage_names': [f"Stage {stage['stage_id']}" for stage in stages],
                'shuffle_read_bytes': [stage['shuffle_read_bytes'] for stage in stages],
                'shuffle_write_bytes': [stage['shuffle_write_bytes'] for stage in stages]
            }

        # Executor 资源数据 - 统一使用字节单位，由JavaScript formatBytes处理
        if result.executors:
            chart_data['executor_resources'] = {
                'executor_ids': [exec.executor_id for exec in result.executors],
                'configured_memory': [exec.configured_memory_bytes for exec in result.executors],  # 保持字节单位
                'actual_memory_used': [exec.max_memory for exec in result.executors],  # 保持字节单位
                'shuffle_read': [exec.total_shuffle_read for exec in result.executors],
                'shuffle_write': [exec.total_shuffle_write for exec in result.executors]
            }

        # 按 Stage 的 Executor Shuffle 使用分布 - 只取有 shuffle 数据的 stage
        stage_executor_data = {}
        if result.shuffle_analysis.stage_shuffle_metrics:
            # 过滤有 shuffle 数据的 stage 并按 shuffle 总量排序
            stages_with_shuffle = []
            for stage_metric in result.shuffle_analysis.stage_shuffle_metrics:
                if stage_metric.executor_shuffle_metrics:
                    total_shuffle = stage_metric.shuffle_read_bytes + stage_metric.shuffle_write_bytes
                    if total_shuffle > 0:  # 只取有 shuffle 数据的 stage
                        stages_with_shuffle.append((stage_metric, total_shuffle))

            # 按 shuffle 总量排序，取前5个
            stages_with_shuffle.sort(key=lambda x: x[1], reverse=True)
            for stage_metric, _ in stages_with_shuffle[:5]:
                stage_key = f"stage_{stage_metric.stage_id}"
                stage_executor_data[stage_key] = {
                    'stage_id': stage_metric.stage_id,
                    'stage_name': stage_metric.stage_name,
                    'executor_ids': list(stage_metric.executor_shuffle_metrics.keys()),
                    'shuffle_read': [metrics['read_bytes'] for metrics in stage_metric.executor_shuffle_metrics.values()],
                    'shuffle_write': [metrics['write_bytes'] for metrics in stage_metric.executor_shuffle_metrics.values()]
                }

        chart_data['stage_executor_shuffle'] = stage_executor_data

        # 数据倾斜数据
        skew_analysis = result.shuffle_analysis.data_skew_analysis
        if skew_analysis.get('stages_with_skew'):
            # 使用第一个有倾斜的 stage 的数据
            first_skewed_stage = skew_analysis['stages_with_skew'][0]
            if result.shuffle_analysis.stage_shuffle_metrics:
                stage_metrics = next(
                    (s for s in result.shuffle_analysis.stage_shuffle_metrics
                     if s.stage_id == first_skewed_stage['stage_id']),
                    None
                )
                if stage_metrics and stage_metrics.executor_shuffle_metrics:
                    executor_reads = []
                    executor_ids = []
                    for exec_id, metrics in stage_metrics.executor_shuffle_metrics.items():
                        executor_ids.append(exec_id)
                        executor_reads.append(metrics['read_bytes'])

                    if executor_reads and max(executor_reads) > 0:
                        avg_read = sum(executor_reads) / len(executor_reads)
                        skew_ratios = [read / avg_read if avg_read > 0 else 1 for read in executor_reads]

                        chart_data['data_skew'] = {
                            'executor_ids': executor_ids,
                            'skew_ratios': skew_ratios
                        }

        # 如果没有倾斜数据，创建默认数据
        if 'data_skew' not in chart_data and result.executors:
            chart_data['data_skew'] = {
                'executor_ids': [exec.executor_id for exec in result.executors[:5]],
                'skew_ratios': [1.0] * min(5, len(result.executors))
            }

        return chart_data

    def _generate_recommendations_html(self, recommendations: List) -> str:
        """生成美观的建议 HTML"""
        if not recommendations:
            return '<div class="no-recommendations"><p style="text-align: center; color: var(--text-muted); font-style: italic;">🎯 暂无优化建议，性能表现良好</p></div>'

        html_parts = []

        for rec_group in recommendations:
            priority_class = f"priority-{rec_group.priority_level.lower()}"

            for rec in rec_group.recommendations:
                config_html = ""
                if rec.get('config'):
                    config_html = f"""
                    <div class="recommendation-config">
                        <span class="recommendation-config-label">配置:</span>
                        {rec.get('config', '')}
                    </div>
                    """

                html_parts.append(f"""
                <div class="recommendation-item">
                    <div class="recommendation-priority {priority_class}">
                        {rec_group.priority_level}
                    </div>
                    <h4 class="recommendation-title">
                        {rec.get('title', '优化建议')}
                    </h4>
                    <p class="recommendation-description">
                        {rec.get('description', '')}
                    </p>
                    <div class="recommendation-suggestion">
                        <div class="recommendation-suggestion-label">建议方案</div>
                        <div class="recommendation-suggestion-text">
                            {rec.get('suggestion', '')}
                        </div>
                    </div>
                    {config_html}
                </div>
                """)

        return "".join(html_parts)

    def _generate_metrics_table(self, result: MatureAnalysisResult) -> str:
        """生成指标表格"""
        if not result.executors:
            return "<p>无 Executor 数据</p>"

        html = """
        <table class="data-table sortable-table">
            <thead>
                <tr>
                    <th class="sortable" onclick="sortTable(0)">Executor ID <span class="sort-indicator">⇅</span></th>
                    <th class="sortable" onclick="sortTable(1)">Host <span class="sort-indicator">⇅</span></th>
                    <th class="sortable" onclick="sortTable(2)">Cores <span class="sort-indicator">⇅</span></th>
                    <th class="sortable" onclick="sortTable(3)">Overhead Memory <span class="sort-indicator">⇅</span></th>
                    <th class="sortable" onclick="sortTable(4)">Shuffle Read <span class="sort-indicator">⇅</span></th>
                    <th class="sortable" onclick="sortTable(5)">Shuffle Write <span class="sort-indicator">⇅</span></th>
                    <th class="sortable" onclick="sortTable(6)">GC Time <span class="sort-indicator">⇅</span></th>
                </tr>
            </thead>
            <tbody>
        """

        for executor in result.executors:
            html += f"""
            <tr>
                <td class="id-cell">{executor.executor_id}</td>
                <td class="host-cell" title="{executor.host}">{executor.host}</td>
                <td class="cores-cell">{executor.cores}</td>
                <td class="memory-cell">{self._format_bytes(executor.overhead_memory)}</td>
                <td class="memory-cell">{self._format_bytes(executor.total_shuffle_read)}</td>
                <td class="memory-cell">{self._format_bytes(executor.total_shuffle_write)}</td>
                <td>{executor.total_gc_time / 1000:.1f}s</td>
            </tr>
            """

        html += "</tbody></table>"
        return html