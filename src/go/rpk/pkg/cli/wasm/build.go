package wasm

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newBuildCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Build a Wasm transform",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := loadCfg(fs)
			out.MaybeDie(err, "unable to find the transform, are you in the same directory as the %q?", configFileName)
			switch cfg.Language {
			case WasmLangTinygo:
				tgo, err := exec.LookPath("tinygo")
				out.MaybeDie(err, "tinygo is not available on $PATH, please download and install it: https://tinygo.org/getting-started/install/")
				c := exec.CommandContext(
					cmd.Context(),
					tgo,
					"build",
					"-target=wasi",
					"-opt=z",
					"-panic=trap",
					"-scheduler=none",
					"-gc=conservative",
					"-o", fmt.Sprintf("%s.wasm", cfg.Name),
					".")
				c.Stderr = os.Stderr
				c.Stdin = os.Stdin
				c.Stdout = os.Stdout
				out.MaybeDieErr(c.Run())
			default:
				out.Die("unknown language: %q", cfg.Language)
			}
			fmt.Println("build succssful 🚀")
			fmt.Println("deploy your wasm function to a topic:")
			fmt.Println("\trpk wasm deploy")
		},
	}
	return cmd
}
