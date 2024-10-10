# notification/email/template_renderer.py

from jinja2 import Environment, FileSystemLoader

class TemplateRenderer:
    def __init__(self, template_dir):
        self.env = Environment(loader=FileSystemLoader(template_dir))

    def render(self, template_name, context):
        print(f"Loading template from: {self.env.loader.searchpath}/{template_name}")
        template = self.env.get_template(template_name)
        return template.render(context)
