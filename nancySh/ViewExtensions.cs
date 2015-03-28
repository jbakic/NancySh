using System;
using Nancy.ViewEngines.Razor;

namespace nancySh
{
    public static class ViewExtensions
    {
        public static IHtmlString PostButton<TModel>(this HtmlHelpers<TModel> html, string action, string caption)
        {
            return html.Raw(string.Format(
@"<form method=""post"" action=""{1}"">
    {0}
    <input type=""submit"" value=""{2}""/>
</form>", html.AntiForgeryToken().ToHtmlString(), action, caption));
        }
    }
}

