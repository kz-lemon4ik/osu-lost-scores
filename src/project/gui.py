import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import generate_image as img_mod
import generate_html as html_mod
from analyzer import scan_replays, make_top

def setup_shcuts(widget):
                                       
    def on_ctrl(event):
        if event.keycode == 67:          
            widget.event_generate("<<Copy>>")
            return "break"
        elif event.keycode == 86:          
            widget.event_generate("<<Paste>>")
            return "break"
        elif event.keycode == 65:          
            widget.event_generate("<<SelectAll>>")
            return "break"
    widget.bind("<Control-KeyPress>", on_ctrl)

def gui_log(msg, log_widget, update_last=False):
                                   
    from datetime import datetime
    stamp = datetime.now().strftime("[%H:%M:%S] ")
    full = stamp + msg
    log_widget.configure(state="normal")
    if update_last:
        content = log_widget.get("1.0", tk.END).splitlines()
        if content:
            content[-1] = full
            log_widget.delete("1.0", tk.END)
            log_widget.insert(tk.END, "\n".join(content) + "\n")
        else:
            log_widget.insert(tk.END, full + "\n")
    else:
        log_widget.insert(tk.END, full + "\n")
    log_widget.see(tk.END)
    log_widget.configure(state="disabled")

def gui_prog(curr, tot, prog_var):
                                
    pct = int((curr / tot)*100) if tot else 0
    prog_var.set(pct)

def pick_dir(game_entry, log_w):
                            
    folder = filedialog.askdirectory(title="Select osu! Game Directory")
    if folder:
        game_entry.delete(0, tk.END)
        game_entry.insert(0, folder)
        gui_log(f"Выбрана папка: {folder}", log_w)

def scan_start(game_dir, prof_url, log_w, prog_var):
                                    
    if not game_dir or not prof_url:
        messagebox.showerror("Ошибка", "Нет папки или URL профиля")
        return
    gui_log("Начинаю анализ реплеев...", log_w)

    def task():
        scan_replays(
            game_dir,
            prof_url,
            lambda msg, update_last=False: gui_log(msg, log_w, update_last),
            lambda c, t: gui_prog(c, t, prog_var)
        )

        messagebox.showinfo("Готово", "Анализ завершён!")

    threading.Thread(target=task, daemon=True).start()

def top_start(game_dir, prof_url, log_w):
                                                   
    if not game_dir or not prof_url:
        messagebox.showerror("Ошибка", "Нет папки или URL профиля")
        return
    gui_log("Создаю потенциальный топ...", log_w)

    def task():
        make_top(
            game_dir,
            prof_url,
            lambda msg, update_last=False: gui_log(msg, log_w, update_last)
        )
        messagebox.showinfo("Готово", "Топ сформирован!")

    threading.Thread(target=task, daemon=True).start()

def img_make(prof_url, log_w):
                                            
    if not prof_url:
        messagebox.showerror("Ошибка", "Нет URL профиля")
        return
    gui_log("Генерация картинок...", log_w)

    def task():
        try:
            token = img_mod.get_token_osu()
            user_data = img_mod.get_user_osu(prof_url, token)
            uid = user_data["id"]
            uname = user_data["username"]
            img_mod.make_img_lost(user_id=uid, user_name=uname)
            img_mod.make_img_top(user_id=uid, user_name=uname)
            gui_log("Картинки созданы (lost_scores_result.png, potential_top_result.png).", log_w)
            messagebox.showinfo("Готово", "Изображения созданы!")
        except Exception as e:
            gui_log(f"Ошибка генерации картинок: {e}", log_w)
            messagebox.showerror("Ошибка", f"Не удалось создать изображения.\n{e}")

    threading.Thread(target=task, daemon=True).start()

def html_make(prof_url, log_w):
                                       
    if not prof_url:
        messagebox.showerror("Ошибка", "Нет URL профиля")
        return
    gui_log("Генерация HTML...", log_w)

    def task():
        try:
            token = img_mod.get_token_osu()
            user_data = img_mod.get_user_osu(prof_url, token)
            uid = user_data["id"]
            uname = user_data["username"]
            html_mod.html_lost(user_id=uid, user_name=uname)
            html_mod.html_top(user_id=uid, user_name=uname)
            gui_log("HTML готов (lost_scores_result.html, potential_top_result.html).", log_w)
            messagebox.showinfo("Готово", "HTML-страницы созданы!")
        except Exception as e:
            gui_log(f"Ошибка генерации HTML: {e}", log_w)
            messagebox.showerror("Ошибка", f"Не удалось создать HTML.\n{e}")

    threading.Thread(target=task, daemon=True).start()

def create_gui():
                                   
    root = tk.Tk()
    root.title("Lost Scores Analyzer")
    root.geometry("800x600")

    main_frame = ttk.Frame(root, padding="10")
    main_frame.pack(fill=tk.BOTH, expand=True)

                
    game_frame = ttk.Frame(main_frame)
    game_frame.pack(fill=tk.X, pady=5)
    ttk.Label(game_frame, text="Game Directory:").pack(side=tk.LEFT)
    game_entry = ttk.Entry(game_frame, width=60)
    game_entry.pack(side=tk.LEFT, padx=5)
    ttk.Button(game_frame, text="Browse", command=lambda: pick_dir(game_entry, log_text)).pack(side=tk.LEFT)
    setup_shcuts(game_entry)

                 
    profile_frame = ttk.Frame(main_frame)
    profile_frame.pack(fill=tk.X, pady=5)
    ttk.Label(profile_frame, text="Player Profile URL:").pack(side=tk.LEFT)
    profile_entry = ttk.Entry(profile_frame, width=60)
    profile_entry.pack(side=tk.LEFT, padx=5)
    setup_shcuts(profile_entry)

              
    progress_var = tk.IntVar()
    progress_bar = ttk.Progressbar(main_frame, variable=progress_var, maximum=100)
    progress_bar.pack(fill=tk.X, pady=5)

         
    log_text = tk.Text(main_frame, height=10, state="normal")
    log_text.pack(fill=tk.BOTH, pady=5)

            
    btns_frame = ttk.Frame(main_frame)
    btns_frame.pack(pady=10)
    ttk.Button(btns_frame, text="Scan Replays",
               command=lambda: scan_start(game_entry.get(), profile_entry.get(), log_text, progress_var)).pack(
        side=tk.LEFT, padx=5)
    ttk.Button(btns_frame, text="Potential Top",
               command=lambda: top_start(game_entry.get(), profile_entry.get(), log_text)).pack(
        side=tk.LEFT, padx=5)
    ttk.Button(btns_frame, text="Generate Image",
               command=lambda: img_make(profile_entry.get(), log_text)).pack(side=tk.LEFT, padx=5)
    ttk.Button(btns_frame, text="Generate HTML",
               command=lambda: html_make(profile_entry.get(), log_text)).pack(side=tk.LEFT, padx=5)

    root.mainloop()
