FROM python:3.7
FROM jupyter/minimal-notebook
WORKDIR $HOME
RUN python -m pip install --upgrade pip
COPY requirements-jupyter.txt ./requirements-jupyter.txt
RUN python -m pip  install -r requirements-jupyter.txt
RUN python -m pip install --upgrade --no-deps --force-reinstall notebook
#
RUN python -m pip install jupyterthemes
RUN python -m pip install --upgrade jupyterthemes
RUN python -m pip install jupyter_contrib_nbextensions
RUN jupyter contrib nbextension install --user
# enable the Nbextensions
RUN jupyter nbextension enable snippets_menu/main
RUN jupyter nbextension enable zenmode/main
RUN jupyter nbextension enable contrib_nbextensions_help_item/main
RUN jupyter nbextension enable autosavetime/main
RUN jupyter nbextension enable codefolding/main
RUN jupyter nbextension enable code_font_size/code_font_size
RUN jupyter nbextension enable code_prettify/code_prettify
RUN jupyter nbextension enable collapsible_headings/main
RUN jupyter nbextension enable comment-uncomment/main
RUN jupyter nbextension enable equation-numbering/main
RUN jupyter nbextension enable execute_time/ExecuteTime
RUN jupyter nbextension enable gist_it/main
RUN jupyter nbextension enable hide_input/main
RUN jupyter nbextension enable spellchecker/main
RUN jupyter nbextension enable toc2/main
RUN jupyter nbextension enable toggle_all_line_numbers/main

RUN jupyter nbextension enable code_prettify/isort
RUN jupyter nbextension enable select_keymap/main
RUN jupyter nbextension enable table_beautifier/main
RUN jupyter nbextension enable move_selected_cells/main
RUN jupyter nbextension enable scratchpad/main
RUN jupyter nbextension enable skip-traceback/main
RUN jupyter nbextension enable tree-filter/index
RUN jupyter nbextension enable varInspector/main
RUN jupyter nbextension enable snippets/main
RUN jupyter nbextension enable scroll_down/main

RUN jupyter nbextension enable navigation-hotkeys/main
RUN jupyter nbextension enable code_prettify/autopep8

RUN jupyter nbextension enable runtools/main
#RUN jupyter nbextension enable keyboard_shortcut_editor/main
# disable help(H)

RUN jupyter nbextension enable livemdpreview/livemdpreview
RUN jupyter nbextension enable help_panel/help_panel
RUN jupyter nbextension enable autoscroll/main
RUN jupyter nbextension enable limit_output/main