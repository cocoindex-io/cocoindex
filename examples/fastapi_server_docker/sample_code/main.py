class Button:
    def __init__(self, label, on_click):
        self.label = label
        self.on_click = on_click

    def click(self):
        print(f"Button '{self.label}' clicked.")
        if callable(self.on_click):
            self.on_click()


class ProgressBar:
    def __init__(self, max_value):
        self.max_value = max_value
        self.current_value = 0

    def update(self, value):
        self.current_value = min(value, self.max_value)
        self.display()

    def display(self):
        percent = (self.current_value / self.max_value) * 100
        print(f"Progress: {percent:.2f}%")


class Slider:
    def __init__(self, min_value, max_value, initial_value=None):
        self.min_value = min_value
        self.max_value = max_value
        self.value = initial_value if initial_value is not None else min_value

    def set_value(self, new_value):
        if self.min_value <= new_value <= self.max_value:
            self.value = new_value
            print(f"Slider set to {self.value}")
        else:
            print("Value out of range.")


class Dropdown:
    def __init__(self, options):
        self.options = options
        self.selected = None

    def select(self, option):
        if option in self.options:
            self.selected = option
            print(f"Dropdown selected: {option}")
        else:
            print("Option not available.")


class TextField:
    def __init__(self, placeholder=''):
        self.placeholder = placeholder
        self.text = ''

    def input(self, new_text):
        self.text = new_text
        print(f"Text field updated: {self.text}")


class Checkbox:
    def __init__(self, label):
        self.label = label
        self.checked = False

    def toggle(self):
        self.checked = not self.checked
        print(f"{self.label}: {'Checked' if self.checked else 'Unchecked'}")


class RadioButton:
    def __init__(self, group, label):
        self.group = group
        self.label = label
        self.selected = False

    def select(self):
        self.selected = True
        print(f"Radio button '{self.label}' selected in group '{self.group}'.")


class ToggleSwitch:
    def __init__(self, state=False):
        self.state = state

    def toggle(self):
        self.state = not self.state
        print(f"ToggleSwitch is now {'On' if self.state else 'Off'}")


class Tooltip:
    def __init__(self, text):
        self.text = text

    def show(self):
        print(f"Tooltip: {self.text}")


class Modal:
    def __init__(self, content):
        self.content = content
        self.visible = False

    def open(self):
        self.visible = True
        print("Modal opened.")
        print(f"Content: {self.content}")

    def close(self):
        self.visible = False
        print("Modal closed.")
