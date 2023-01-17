def _show_on_single_plot(ax):
    for p in bar_plot.patches:
        _x = p.get_x() + p.get_width() / 2
        _y = p.get_y() + p.get_height() + 100
        value = format(int(p.get_height()))
        ax.text(_x, _y, value, ha="center")


def currency(x, pos):
    'The two args are the value and tick position'
    if x >= 1000000:
        return '${:1.1f}M'.format(x * 1e-6)
    return '${:1.0f}K'.format(x * 1e-3)
