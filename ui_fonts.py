# -*- coding: utf-8 -*-
"""プラグインの文字の大きさ。

WebODM Importer の窓は QGIS のフォントサイズ設定に関係なく一つの大きさ
（PLUGIN_PT）で表示し、レイアウトが崩れないようにする。OS の表示倍率
（Windows の 125 %、高解像度画面）は窓全体にそのまま効く。書体は QGIS
（システム）のまま。Qt は窓のフォントを、その窓が開く別の窓へは引き継がない
ため、スタイルシートでも同じ大きさを渡す（quick_landscape_analysis_explorer・
forestry_operations_lite の ui_fonts と同じ方式）。
"""

from qgis.PyQt.QtGui import QFont
from qgis.PyQt.QtWidgets import QApplication

PLUGIN_PT = 9.0


def plugin_font():
    font = QFont(QApplication.font())
    font.setPointSizeF(PLUGIN_PT)
    return font


# 窓の中のすべてに効く大きさをスタイルシートで渡す。QGIS はアプリ全体の
# スタイルシートやフォントで文字の大きさを決めているため、フォントだけでは
# 上書きされる。スタイルシートなら、フォントだけでは届かないもの（コンボの一覧、
# 窓が開くダイアログ・メッセージ、ツールチップ）にも届く。部品ごとのスタイル
# シートで指定した大きさ（注記など）はそちらが優先される。
WINDOW_RULES = "QWidget {{ font-size: {0:g}pt; }} QToolTip {{ font-size: {0:g}pt; }}".format(
    PLUGIN_PT
)


def use_plugin_font(window):
    """窓（とその中身・一覧・ツールチップ）にプラグインの文字の大きさを付ける。"""

    window.setFont(plugin_font())
    if WINDOW_RULES not in window.styleSheet():
        window.setStyleSheet((window.styleSheet() + " " + WINDOW_RULES).strip())
    return window
