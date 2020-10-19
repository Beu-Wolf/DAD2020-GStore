namespace PuppetMaster
{
    partial class PuppetMasterForm
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.CommandBox = new System.Windows.Forms.TextBox();
            this.executeButton = new System.Windows.Forms.Button();
            this.logBox = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // CommandBox
            // 
            this.CommandBox.Font = new System.Drawing.Font("Microsoft Sans Serif", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.CommandBox.Location = new System.Drawing.Point(12, 12);
            this.CommandBox.Name = "CommandBox";
            this.CommandBox.Size = new System.Drawing.Size(672, 34);
            this.CommandBox.TabIndex = 0;
            this.CommandBox.KeyDown += new System.Windows.Forms.KeyEventHandler(this.CommandBox_KeyDown);
            // 
            // executeButton
            // 
            this.executeButton.Location = new System.Drawing.Point(690, 12);
            this.executeButton.Name = "executeButton";
            this.executeButton.Size = new System.Drawing.Size(98, 34);
            this.executeButton.TabIndex = 1;
            this.executeButton.Text = "Run Script";
            this.executeButton.UseVisualStyleBackColor = true;
            this.executeButton.Click += new System.EventHandler(this.RunScriptButton_Click);
            // 
            // logBox
            // 
            this.logBox.Location = new System.Drawing.Point(12, 57);
            this.logBox.Multiline = true;
            this.logBox.Name = "logBox";
            this.logBox.ReadOnly = true;
            this.logBox.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.logBox.Size = new System.Drawing.Size(776, 381);
            this.logBox.TabIndex = 2;
            // 
            // PuppetMasterForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.logBox);
            this.Controls.Add(this.executeButton);
            this.Controls.Add(this.CommandBox);
            this.Name = "PuppetMasterForm";
            this.Text = "PuppetMaster";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox CommandBox;
        private System.Windows.Forms.Button executeButton;
        private System.Windows.Forms.TextBox logBox;
    }
}

